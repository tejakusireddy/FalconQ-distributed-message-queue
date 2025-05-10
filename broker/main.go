// broker/main.go

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
	"github.com/stathat/consistent"
)

const DefaultNumPartitions = 3 // Default partitions per topic
const MaxNumPartitions = 256   // Safety limit for partitions

var cluster *ClusterManager

// --- API Error Structure ---
type ApiError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func NewApiError(errType, message, details string) ApiError {
	return ApiError{Type: errType, Message: message, Details: details}
}

// --- Core Data Structures ---

// Message defines the structure for incoming API messages
type Message struct {
	Value        string  `json:"message"`
	Priority     string  `json:"priority"`               // Expected: "high" or "low"
	PartitionKey *string `json:"partitionKey,omitempty"` // Optional key for hashing
}

// StoredMessage defines how messages are stored in BadgerDB and returned in API
type StoredMessage struct {
	Value    string `json:"v"`
	Priority string `json:"p"`
	Offset   uint64 `json:"-"` // Offset added for returning to client, not stored in value
}

// Partition holds state related to a partition. Offsets are persisted.
type Partition struct {
	ID        int
	TopicName string
	db        *badger.DB
	offsetSeq *badger.Sequence // Sequence for message offsets
	// TODO: Add retention policy logic later
}

// Topic holds multiple partitions.
type Topic struct {
	Name              string
	Partitions        map[int]*Partition // map[partitionID]*Partition
	mu                sync.RWMutex       // Protects Partitions map & roundRobinCounter
	db                *badger.DB
	NumPartitions     int                    // Number of partitions for this topic
	HashRing          *consistent.Consistent // Consistent hash ring for partition selection
	roundRobinCounter uint64                 // Fallback counter if no partition key
}

// Broker manages topics.
type Broker struct {
	Topics map[string]*Topic // map[topicName]*Topic
	mu     sync.RWMutex      // Protects Topics map
	db     *badger.DB        // Holds the BadgerDB instance
}

// --- Global Broker Instance ---
var broker *Broker // Initialized in main

// --- Initialization ---

// NewBroker creates a new Broker instance with the BadgerDB handle.
func NewBroker(db *badger.DB) *Broker {
	return &Broker{
		Topics: make(map[string]*Topic),
		db:     db,
	}
}

// NewTopic creates a new topic structure with consistent hashing ring.
func NewTopic(name string, db *badger.DB, numPartitions int) (*Topic, error) {
	if numPartitions <= 0 {
		numPartitions = DefaultNumPartitions
	}
	if numPartitions > MaxNumPartitions {
		return nil, fmt.Errorf("requested number of partitions %d exceeds maximum limit %d", numPartitions, MaxNumPartitions)
	}

	members := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		members[i] = strconv.Itoa(i)
	}
	hashRing := consistent.New()
	hashRing.NumberOfReplicas = 20 // More replicas for better distribution
	hashRing.Set(members)          // Add partitions "0", "1", ..., "N-1"

	return &Topic{
		Name:          name,
		Partitions:    make(map[int]*Partition),
		mu:            sync.RWMutex{},
		db:            db,
		NumPartitions: numPartitions,
		HashRing:      hashRing,
	}, nil
}

// InitializePartition creates a partition struct and its message offset sequence.
func InitializePartition(id int, topicName string, db *badger.DB) (*Partition, error) {
	seqKey := []byte(fmt.Sprintf("t/%s/p/%d/_seq", topicName, id)) // Sequence key for messages
	// Bandwidth = 100: Fetch 100 sequence numbers at a time for performance
	seq, err := db.GetSequence(seqKey, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to get message sequence for topic '%s' partition %d: %w", topicName, id, err)
	}
	return &Partition{
		ID:        id,
		TopicName: topicName,
		db:        db,
		offsetSeq: seq,
	}, nil
}

// --- Broker/Topic Methods ---

// getOrCreateTopic retrieves an existing topic or creates a new one safely.
func (b *Broker) getOrCreateTopic(topicName string) (*Topic, error) {
	b.mu.RLock()
	topic, exists := b.Topics[topicName]
	b.mu.RUnlock()
	if exists {
		return topic, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	topic, exists = b.Topics[topicName] // Double-check
	if exists {
		return topic, nil
	}

	var err error
	topic, err = NewTopic(topicName, b.db, DefaultNumPartitions)
	if err != nil {
		return nil, fmt.Errorf("failed to create new topic %s: %w", topicName, err)
	}

	log.Printf("✨ Topic '%s' created with %d partitions.", topicName, DefaultNumPartitions)
	b.Topics[topicName] = topic
	return topic, nil
}

// getOrCreatePartition retrieves or creates a partition within a topic safely.
func (t *Topic) getOrCreatePartition(partitionID int) (*Partition, error) {
	if partitionID < 0 || partitionID >= t.NumPartitions {
		return nil, fmt.Errorf("invalid partition ID %d for topic %s (max %d)", partitionID, t.Name, t.NumPartitions-1)
	}

	t.mu.RLock()
	partition, exists := t.Partitions[partitionID]
	t.mu.RUnlock()
	if exists {
		return partition, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	partition, exists = t.Partitions[partitionID] // Double-check
	if exists {
		return partition, nil
	}
	log.Printf("🔧 Partition %d created for topic '%s'.", partitionID, t.Name)
	var err error
	partition, err = InitializePartition(partitionID, t.Name, t.db)
	if err != nil {
		log.Printf("❌ Failed to initialize partition %d for topic '%s': %v", partitionID, t.Name, err)
		return nil, err
	}
	t.Partitions[partitionID] = partition
	return partition, nil
}

// Close releases resources held by the broker (DB connection, sequences).
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	log.Printf("Releasing sequences for %d topics...", len(b.Topics))
	topicsToClose := make([]*Topic, 0, len(b.Topics))
	for _, topic := range b.Topics {
		topicsToClose = append(topicsToClose, topic)
	}

	for _, topic := range topicsToClose {
		topic.mu.Lock()
		partitionsToClose := make([]*Partition, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitionsToClose = append(partitionsToClose, partition)
		}
		topic.mu.Unlock()

		for _, partition := range partitionsToClose {
			if partition.offsetSeq != nil {
				if err := partition.offsetSeq.Release(); err != nil {
					log.Printf("WARN: Failed to release sequence for topic %s partition %d: %v", topic.Name, partition.ID, err)
				}
			}
		}
	}
	log.Println("Closing BadgerDB...")
	return b.db.Close()
}

// --- Partition Methods ---

// GenerateMessageKey creates the BadgerDB key for a message offset.
// Key Format: t/<topicName>/p/<partitionID>/m/<offset_bytes>
func GenerateMessageKey(topicName string, partitionID int, offset uint64) []byte {
	keyPrefix := fmt.Sprintf("t/%s/p/%d/m/", topicName, partitionID)
	key := make([]byte, len(keyPrefix)+8)
	copy(key, []byte(keyPrefix))
	binary.BigEndian.PutUint64(key[len(keyPrefix):], offset)
	return key
}

// GenerateConsumerOffsetKey creates the BadgerDB key for a consumer offset.
// Key Format: t/<topicName>/p/<partitionID>/c/<consumerID>
func GenerateConsumerOffsetKey(topicName string, partitionID int, consumerID string) []byte {
	return []byte(fmt.Sprintf("t/%s/p/%d/c/%s", topicName, partitionID, consumerID))
}

// GetOffsetFromMessageKey extracts the offset from a message key.
func GetOffsetFromMessageKey(key []byte, prefixLen int) (uint64, error) {
	if len(key) != prefixLen+8 {
		return 0, fmt.Errorf("invalid key length: got %d, expected %d", len(key), prefixLen+8)
	}
	return binary.BigEndian.Uint64(key[prefixLen:]), nil
}

// AppendMessage adds a message to the partition's persistent log using BadgerDB Sequence for offset.
func (p *Partition) AppendMessage(value string, priority string) (uint64, error) {
	if p.offsetSeq == nil {
		return 0, fmt.Errorf("sequence generator not initialized for partition %d", p.ID)
	}
	offset, err := p.offsetSeq.Next()
	if err != nil {
		return 0, fmt.Errorf("failed to get next offset for p%d: %w", p.ID, err)
	}

	key := GenerateMessageKey(p.TopicName, p.ID, offset)
	msgData := StoredMessage{Value: value, Priority: priority}
	valueBytes, err := json.Marshal(msgData)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal message for p%d offset %d: %w", p.ID, offset, err)
	}

	err = p.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, valueBytes)
	})
	if err != nil {
		log.Printf("WARN: Failed to write message for p%d at offset %d after getting sequence number: %v", p.ID, offset, err)
		return 0, fmt.Errorf("failed BadgerDB update for p%d offset %d: %w", p.ID, offset, err)
	}
	return offset, nil
}

// ReadMessages reads a batch of messages using BadgerDB iterators.
func (p *Partition) ReadMessages(startOffset uint64, batchSize int) ([]StoredMessage, uint64, error) {
	messages := make([]StoredMessage, 0, batchSize)
	var nextOffset uint64 = startOffset // Default if no messages found >= startOffset

	err := p.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = batchSize * 2 // Prefetch more

		it := txn.NewIterator(opts)
		defer it.Close()

		keyPrefixBytes := []byte(fmt.Sprintf("t/%s/p/%d/m/", p.TopicName, p.ID))
		startKey := GenerateMessageKey(p.TopicName, p.ID, startOffset)

		lastProcessedOffset := startOffset - 1 // Track the last valid offset read

		it.Seek(startKey)

		for ; it.ValidForPrefix(keyPrefixBytes) && len(messages) < batchSize; it.Next() {
			item := it.Item()
			key := item.Key()

			currentKeyOffset, err := GetOffsetFromMessageKey(key, len(keyPrefixBytes))
			if err != nil {
				log.Printf("WARN: Skipping key with unexpected format in p%d: %s (%v)", p.ID, key, err)
				continue // Skip malformed key
			}

			var msg StoredMessage
			err = item.Value(func(val []byte) error {
				if err := json.Unmarshal(val, &msg); err != nil {
					log.Printf("WARN: Failed to unmarshal message at p%d offset %d: %v", p.ID, currentKeyOffset, err)
					return nil // Treat as skippable
				}
				return nil
			})
			if err != nil {
				// This error is from Badger interaction, potentially serious
				return fmt.Errorf("error reading value for p%d offset %d: %w", p.ID, currentKeyOffset, err)
			}

			if msg.Value == "" && msg.Priority == "" { // Check if unmarshal failed silently
				continue // Skip if unmarshal failed
			}

			msg.Offset = currentKeyOffset // Add offset to struct before appending
			messages = append(messages, msg)
			lastProcessedOffset = currentKeyOffset
		}
		// Determine the offset for the *next* read attempt
		nextOffset = lastProcessedOffset + 1
		return nil // End of transaction
	})

	if err != nil {
		return nil, startOffset, fmt.Errorf("failed during batch read for p%d: %w", p.ID, err)
	}
	return messages, nextOffset, nil
}

// GetConsumerOffset reads the persisted offset for a consumer on this partition.
func (p *Partition) GetConsumerOffset(consumerID string) (uint64, error) {
	key := GenerateConsumerOffsetKey(p.TopicName, p.ID, consumerID)
	var offset uint64 = 0 // Default to 0 if key not found
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Key not found means offset is 0, not an error
			}
			return err // Other DB error
		}
		err = item.Value(func(val []byte) error {
			if len(val) != 8 {
				log.Printf("ERROR: Corrupted offset value for consumer '%s' on topic '%s' p%d. Length: %d. Resetting to 0.", consumerID, p.TopicName, p.ID, len(val))
				offset = 0 // Default to 0 on corruption
				return nil
			}
			offset = binary.BigEndian.Uint64(val)
			return nil
		})
		return err // Return error from item.Value (if any)
	})

	if err != nil {
		return 0, fmt.Errorf("failed to get consumer offset for '%s' on '%s'/p%d: %w", consumerID, p.TopicName, p.ID, err)
	}
	return offset, nil
}

// SetConsumerOffset persists the offset for a consumer on this partition.
func (p *Partition) SetConsumerOffset(consumerID string, offset uint64) error {
	key := GenerateConsumerOffsetKey(p.TopicName, p.ID, consumerID)
	valBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valBytes, offset)

	err := p.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, valBytes)
	})

	if err != nil {
		return fmt.Errorf("failed to set consumer offset for '%s' on '%s'/p%d to %d: %w", consumerID, p.TopicName, p.ID, offset, err)
	}
	return nil
}

// --- Helper Functions ---
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- Main Function ---
func main() {
	fmt.Println("Starting FalconQ Broker...")

	configPath := flag.String("config", "config.yaml", "Path to config file")

	flag.Parse()

	// Load config.yaml
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize cluster
	cluster = NewClusterManager(cfg)

	// Open DB
	// dbPath := "falconq_data"
	dbPath := fmt.Sprintf("falconq_data_%s", cfg.NodeID)
	port := fmt.Sprintf(":%d", cfg.HTTPPort)

	fmt.Printf("Opening BadgerDB at path: %s\n", dbPath)
	opts := badger.DefaultOptions(dbPath).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("❌ Failed to open BadgerDB: %v", err)
	}
	broker = NewBroker(db)
	fmt.Println("Broker initialized.")
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	// 🔁 Register replication handler before starting server
	r.POST("/internal/replicate", handleReplication)

	r.POST("/topic/:topic/publish", handlePublish)
	r.GET("/topic/:topic/peek", handlePeek)
	r.GET("/topic/:topic/consume", handleConsume)
	r.GET("/topics", handleGetTopics)
	r.GET("/topics/:topic/partitions", handleGetPartitions)
	// port := ":8080"

	srv := &http.Server{Addr: port, Handler: r}
	go func() {
		fmt.Printf("🚀 FalconQ Broker API running at http://localhost%s\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("🚦 Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("❌ Server forced to shutdown:", err)
	}
	log.Println("🧹 Closing broker resources...")
	if broker != nil {
		if err := broker.Close(); err != nil {
			log.Printf("❌ Error closing broker resources: %v\n", err)
		} else {
			log.Println("✅ Broker resources closed.")
		}
	} else {
		if db != nil {
			db.Close()
		}
	}
	log.Println("👋 Server exiting")
}

// --- Request Handlers ---

func handlePublish(c *gin.Context) {
	topicName := c.Param("topic")
	var msg Message
	if err := c.BindJSON(&msg); err != nil {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid message format", err.Error()))
		return
	}
	if msg.Priority != "high" && msg.Priority != "low" {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid priority value", "Priority must be 'high' or 'low'"))
		return
	}
	topic, err := broker.getOrCreateTopic(topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access topic", err.Error()))
		return
	}

	// Determine Target Partition ID
	var targetPartitionID int
	if msg.PartitionKey != nil && *msg.PartitionKey != "" {
		partitionStr, err := topic.HashRing.Get(*msg.PartitionKey)
		if err != nil {
			log.Printf("WARN: Consistent hash error for topic %s key '%s': %v. Falling back to round-robin.", topicName, *msg.PartitionKey, err)
			topic.mu.Lock()
			targetPartitionID = int(topic.roundRobinCounter % uint64(topic.NumPartitions))
			topic.roundRobinCounter++
			topic.mu.Unlock()
		} else {
			targetPartitionID, err = strconv.Atoi(partitionStr)
			if err != nil {
				log.Printf("WARN: Consistent hash returned non-int '%s' for topic %s key '%s': %v. Falling back to round-robin.", partitionStr, topicName, *msg.PartitionKey, err)
				topic.mu.Lock()
				targetPartitionID = int(topic.roundRobinCounter % uint64(topic.NumPartitions))
				topic.roundRobinCounter++
				topic.mu.Unlock()
			}
		}
	} else {
		topic.mu.Lock()
		targetPartitionID = int(topic.roundRobinCounter % uint64(topic.NumPartitions))
		topic.roundRobinCounter++
		topic.mu.Unlock()
	}

	partition, err := topic.getOrCreatePartition(targetPartitionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access target partition", err.Error()))
		return
	}
	offset, err := partition.AppendMessage(msg.Value, msg.Priority)
	if err != nil {
		log.Printf("❌ Failed to append message to topic '%s' p%d: %v", topicName, targetPartitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to store message", err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "message received", "topic": topicName, "partitionID": targetPartitionID, "offset": offset, "priority": msg.Priority})
}

func handlePeek(c *gin.Context) {
	topicName := c.Param("topic")
	offsetQuery := c.DefaultQuery("offset", "0")
	batchQuery := c.DefaultQuery("batch", "1")
	partitionIDStr := c.DefaultQuery("partitionID", "-1")

	startOffset, err := strconv.ParseUint(offsetQuery, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid offset value", "Offset must be a non-negative integer"))
		return
	}
	batchSize, err := strconv.Atoi(batchQuery)
	if err != nil || batchSize <= 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid batch size", "Batch size must be a positive integer"))
		return
	}
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil || partitionID < 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Missing or invalid partitionID", "partitionID query parameter (non-negative integer) is required for peek/consume"))
		return
	}

	topic, err := broker.getOrCreateTopic(topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access topic", err.Error()))
		return
	}
	if partitionID >= topic.NumPartitions {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid partitionID", fmt.Sprintf("Partition ID must be less than %d for topic %s", topic.NumPartitions, topicName)))
		return
	}

	partition, err := topic.getOrCreatePartition(partitionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access specified partition", err.Error()))
		return
	}

	// Read raw batch from the specified partition
	rawMessages, nextOffsetRead, err := partition.ReadMessages(startOffset, batchSize)
	if err != nil {
		log.Printf("❌ Failed to read messages for peek on topic '%s' p%d: %v", topicName, partitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to read messages", err.Error()))
		return
	}

	// Priority Filtering
	messagesToSend := make([]StoredMessage, 0, len(rawMessages))
	for _, msg := range rawMessages {
		if msg.Priority == "high" {
			messagesToSend = append(messagesToSend, msg)
		}
	}
	for _, msg := range rawMessages {
		if len(messagesToSend) >= batchSize {
			break
		}
		if msg.Priority == "low" {
			messagesToSend = append(messagesToSend, msg)
		}
	}

	// Determine client's next offset based on actual last message sent
	finalClientNextOffset := startOffset
	if len(messagesToSend) > 0 {
		finalClientNextOffset = messagesToSend[len(messagesToSend)-1].Offset + 1
	} else {
		finalClientNextOffset = nextOffsetRead
	}

	c.JSON(http.StatusOK, gin.H{"topic": topicName, "partitionID": partitionID, "messages": messagesToSend, "startOffset": startOffset, "count": len(messagesToSend), "nextOffset": finalClientNextOffset})
}

func handleConsume(c *gin.Context) {
	topicName := c.Param("topic")
	consumerID := c.Query("consumerID")
	batchQuery := c.DefaultQuery("batch", "1")
	partitionIDStr := c.DefaultQuery("partitionID", "-1")

	if consumerID == "" {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Missing consumerID", "consumerID query parameter is required"))
		return
	}
	batchSize, err := strconv.Atoi(batchQuery)
	if err != nil || batchSize <= 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid batch size", "Batch size must be a positive integer"))
		return
	}
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil || partitionID < 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Missing or invalid partitionID", "partitionID query parameter (non-negative integer) is required for peek/consume"))
		return
	}

	topic, err := broker.getOrCreateTopic(topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access topic", err.Error()))
		return
	}
	if partitionID >= topic.NumPartitions {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid partitionID", fmt.Sprintf("Partition ID must be less than %d for topic %s", topic.NumPartitions, topicName)))
		return
	}

	partition, err := topic.getOrCreatePartition(partitionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access specified partition", err.Error()))
		return
	}

	// Get Current Offset from BadgerDB
	currentConsumerOffset, err := partition.GetConsumerOffset(consumerID)
	if err != nil {
		log.Printf("❌ Failed to get consumer offset for consumer '%s' on topic '%s' p%d: %v", consumerID, topicName, partitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to retrieve consumer offset", err.Error()))
		return
	}

	// Read Raw Batch
	rawMessages, nextOffsetRead, err := partition.ReadMessages(currentConsumerOffset, batchSize)
	if err != nil {
		log.Printf("❌ Failed to read messages for consume on topic '%s' p%d for consumer '%s': %v", topicName, partitionID, consumerID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to read messages", err.Error()))
		return
	}

	// Priority Filtering
	messagesToSend := make([]StoredMessage, 0, len(rawMessages))
	for _, msg := range rawMessages {
		if msg.Priority == "high" {
			messagesToSend = append(messagesToSend, msg)
		}
	}
	for _, msg := range rawMessages {
		if len(messagesToSend) >= batchSize {
			break
		}
		if msg.Priority == "low" {
			messagesToSend = append(messagesToSend, msg)
		}
	}

	// Determine and Update Consumer Offset in BadgerDB
	var finalConsumerNextOffset uint64
	if len(messagesToSend) > 0 {
		finalConsumerNextOffset = messagesToSend[len(messagesToSend)-1].Offset + 1
	} else {
		finalConsumerNextOffset = nextOffsetRead
	}

	err = partition.SetConsumerOffset(consumerID, finalConsumerNextOffset)
	if err != nil {
		// Log error, but continue request for now. See previous comments on robustness.
		log.Printf("ERROR: Failed to set consumer offset for consumer '%s' on topic '%s' p%d to %d: %v", consumerID, topicName, partitionID, finalConsumerNextOffset, err)
	}

	c.JSON(http.StatusOK, gin.H{"consumerID": consumerID, "topic": topicName, "partitionID": partitionID, "messages": messagesToSend, "startOffset": currentConsumerOffset, "count": len(messagesToSend), "nextOffset": finalConsumerNextOffset})
}

// --- Admin Handlers ---

func handleGetTopics(c *gin.Context) {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	topicNames := make([]string, 0, len(broker.Topics))
	for name := range broker.Topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)
	c.JSON(http.StatusOK, gin.H{"topics": topicNames})
}

type PartitionInfo struct {
	ID int `json:"id"`
}

func handleGetPartitions(c *gin.Context) {
	topicName := c.Param("topic")
	broker.mu.RLock()
	topic, topicExists := broker.Topics[topicName]
	broker.mu.RUnlock()
	if !topicExists {
		c.JSON(http.StatusNotFound, NewApiError("NotFoundError", "Topic not found", fmt.Sprintf("Topic '%s' does not exist", topicName)))
		return
	}
	topic.mu.RLock()
	defer topic.mu.RUnlock()
	partitionInfos := make([]PartitionInfo, 0, len(topic.Partitions))
	partitionIDs := make([]int, 0, len(topic.Partitions))
	for id := range topic.Partitions {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Ints(partitionIDs)
	for _, id := range partitionIDs {
		partitionInfos = append(partitionInfos, PartitionInfo{ID: id})
	}
	c.JSON(http.StatusOK, gin.H{"topic": topicName, "configuredPartitions": topic.NumPartitions, "partitionKeyStrategy": "consistent_hash+round_robin_fallback", "activePartitions": partitionInfos})
}

// -- Replication across 3 Nodes

type Peer struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

type Config struct {
	NodeID   string `yaml:"node_id"`
	HTTPPort int    `yaml:"http_port"`
	Peers    []Peer `yaml:"peers"`
}

type ClusterManager struct {
	NodeID string
	Peers  map[string]string // map[peerID]address
	Client *http.Client
}

func NewClusterManager(cfg *Config) *ClusterManager {
	peers := make(map[string]string)
	for _, p := range cfg.Peers {
		peers[p.ID] = p.Address
	}
	return &ClusterManager{
		NodeID: cfg.NodeID,
		Peers:  peers,
		Client: &http.Client{Timeout: 2 * time.Second},
	}
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

type ReplicationRequest struct {
	Topic        string `json:"topic"`
	PartitionID  int    `json:"partitionID"`
	Value        string `json:"value"`
	Priority     string `json:"priority"`
	OriginNodeID string `json:"originNodeID"`
}

func handleReplication(c *gin.Context) {
	var req ReplicationRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid replication request", err.Error()))
		return
	}

	// Skip if replicated from self
	if req.OriginNodeID == cluster.NodeID {
		c.JSON(http.StatusOK, gin.H{"status": "ignored (from self)"})
		return
	}

	topic, err := broker.getOrCreateTopic(req.Topic)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access topic", err.Error()))
		return
	}
	partition, err := topic.getOrCreatePartition(req.PartitionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access partition", err.Error()))
		return
	}
	_, err = partition.AppendMessage(req.Value, req.Priority)
	if err != nil {
		log.Printf("❌ Replication append failed: %v", err)
		c.JSON(http.StatusInternalServerError, NewApiError("ReplicationError", "Failed to append replicated message", err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "replicated"})
}
