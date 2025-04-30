package main

import (
	// "errors" // Removed - was not used
	"fmt"
	"net/http"
	"sort" // To return sorted topic lists
	"strconv"
	"sync"

	"github.com/gin-gonic/gin"
)

// --- API Error Structure ---

// ApiError defines a standard structure for API error responses.
type ApiError struct {
	Type    string `json:"type"`              // e.g., "ValidationError", "NotFoundError", "ServerError"
	Message string `json:"message"`           // User-friendly error message
	Details string `json:"details,omitempty"` // Optional technical details
}

// Helper to create a new ApiError
func NewApiError(errType, message, details string) ApiError {
	return ApiError{Type: errType, Message: message, Details: details}
}

// --- Core Data Structures ---

// Message defines the structure for incoming messages
type Message struct {
	Value    string `json:"message"`
	Priority string `json:"priority"` // Expected: "high" or "low"
}

// Partition holds the data and state for a single partition within a topic.
type Partition struct {
	ID              int                // Added ID for clarity in responses
	HighMessages    []string           // High priority messages log (append-only for now)
	LowMessages     []string           // Low priority messages log (append-only for now)
	ConsumerOffsets map[string]int     // Tracks the next offset for each consumerID for this partition
	mu              sync.RWMutex       // Protects this specific partition's data
	// TODO: Add retention policy or auto-clean based on offsets/time. Slices grow indefinitely.
}

// NewPartition creates a new partition structure.
func NewPartition(id int) *Partition {
	return &Partition{
		ID:              id,
		HighMessages:    make([]string, 0),
		LowMessages:     make([]string, 0),
		ConsumerOffsets: make(map[string]int),
	}
}

// Topic holds multiple partitions and manages access to them.
type Topic struct {
	Name       string
	Partitions map[int]*Partition // map[partitionID]*Partition
	mu         sync.RWMutex       // Protects the Partitions map itself (adding/removing partitions)
}

// NewTopic creates a new topic structure.
func NewTopic(name string) *Topic {
	return &Topic{
		Name:       name,
		Partitions: make(map[int]*Partition),
	}
}

// Broker is the top-level structure managing all topics.
type Broker struct {
	Topics map[string]*Topic // map[topicName]*Topic
	mu     sync.RWMutex    // Protects the Topics map itself (adding/deleting topics)
}

// Global broker instance
var broker = Broker{Topics: make(map[string]*Topic)}

// --- Helper Functions ---

// getOrCreateTopic retrieves an existing topic or creates a new one safely.
func (b *Broker) getOrCreateTopic(topicName string) *Topic {
	b.mu.RLock()
	topic, exists := b.Topics[topicName]
	b.mu.RUnlock()
	if exists {
		return topic
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	topic, exists = b.Topics[topicName] // Double-check
	if exists {
		return topic
	}
	fmt.Printf("✨ Topic '%s' created.\n", topicName)
	topic = NewTopic(topicName)
	b.Topics[topicName] = topic
	return topic
}

// getOrCreatePartition retrieves or creates a partition within a topic safely.
func (t *Topic) getOrCreatePartition(partitionID int) *Partition {
	t.mu.RLock()
	partition, exists := t.Partitions[partitionID]
	t.mu.RUnlock()
	if exists {
		return partition
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	partition, exists = t.Partitions[partitionID] // Double-check
	if exists {
		return partition
	}
	fmt.Printf("🔧 Partition %d created for topic '%s'.\n", partitionID, t.Name)
	partition = NewPartition(partitionID)
	t.Partitions[partitionID] = partition
	return partition
}

// combineMessages combines messages from a partition respecting priority.
// NOTE: Still potentially inefficient for very large partitions.
func (p *Partition) combineMessages() []string {
	// Assumes caller holds at least a Read Lock on p.mu
	combined := make([]string, 0, len(p.HighMessages)+len(p.LowMessages))
	combined = append(combined, p.HighMessages...)
	combined = append(combined, p.LowMessages...)
	return combined
}

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- Main Function ---

func main() {
	fmt.Println("Starting Gin router...")
	r := gin.Default()

	// === API Endpoints ===
	// Core Pub/Sub
	r.POST("/topic/:topic/publish", handlePublish)
	r.GET("/topic/:topic/peek", handlePeek)
	r.GET("/topic/:topic/consume", handleConsume)

	// Introspection/Admin
	r.GET("/topics", handleGetTopics)
	r.GET("/topics/:topic/partitions", handleGetPartitions)

	// === Start Server ===
	port := ":8080"
	fmt.Printf("Broker API running at http://localhost%s\n", port)
	if err := r.Run(port); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}

// --- Request Handlers ---

// handlePublish adds a message to the appropriate partition (currently always partition 0).
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

	topic := broker.getOrCreateTopic(topicName)
	// TODO: Implement partitioning logic here instead of hardcoding 0
	partitionID := 0
	partition := topic.getOrCreatePartition(partitionID)

	partition.mu.Lock()
	if msg.Priority == "high" {
		partition.HighMessages = append(partition.HighMessages, msg.Value)
	} else {
		partition.LowMessages = append(partition.LowMessages, msg.Value)
	}
	partition.mu.Unlock()

	fmt.Printf("📤 Added %s priority to '%s' [P%d]: %s\n", msg.Priority, topicName, partitionID, msg.Value)
	c.JSON(http.StatusOK, gin.H{
		"status":      "message received",
		"topic":       topicName,
		"partitionID": partitionID,
		"message":     msg.Value,
		"priority":    msg.Priority,
	})
}

// handlePeek reads messages after an offset without consuming. Uses Read Lock.
func handlePeek(c *gin.Context) {
	topicName := c.Param("topic")
	// TODO: Allow specifying partitionID in query later
	partitionID := 0
	offsetQuery := c.DefaultQuery("offset", "0")
	batchQuery := c.DefaultQuery("batch", "1")

	currentOffset, err := strconv.Atoi(offsetQuery)
	if err != nil || currentOffset < 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid offset value", "Offset must be a non-negative integer"))
		return
	}
	batchSize, err := strconv.Atoi(batchQuery)
	if err != nil || batchSize <= 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid batch size", "Batch size must be a positive integer"))
		return
	}

	broker.mu.RLock()
	topic, topicExists := broker.Topics[topicName]
	broker.mu.RUnlock()
	if !topicExists {
		c.JSON(http.StatusNotFound, NewApiError("NotFoundError", "Topic not found", fmt.Sprintf("Topic '%s' does not exist", topicName)))
		return
	}

	topic.mu.RLock()
	partition, partitionExists := topic.Partitions[partitionID]
	topic.mu.RUnlock()
	if !partitionExists {
		// If the topic exists but partition 0 doesn't (edge case?), treat as empty
		c.JSON(http.StatusOK, gin.H{"topic": topicName, "partitionID": partitionID, "messages": []string{}, "nextOffset": currentOffset, "info": "partition not found or empty"})
		return
	}

	partition.mu.RLock()
	combinedMessages := partition.combineMessages()
	totalAvailable := len(combinedMessages)
	partition.mu.RUnlock()

	if currentOffset >= totalAvailable {
		c.JSON(http.StatusOK, gin.H{"topic": topicName, "partitionID": partitionID, "messages": []string{}, "nextOffset": currentOffset, "info": "no new messages available at this offset"})
		return
	}

	endIndex := min(currentOffset+batchSize, totalAvailable)
	messagesToSend := combinedMessages[currentOffset:endIndex]
	nextOffset := endIndex

	c.JSON(http.StatusOK, gin.H{
		"topic":       topicName,
		"partitionID": partitionID,
		"messages":    messagesToSend,
		"startOffset": currentOffset,
		"count":       len(messagesToSend),
		"nextOffset":  nextOffset,
	})
}

// handleConsume reads messages for a consumer, updating offset. Uses Write Lock.
func handleConsume(c *gin.Context) {
	topicName := c.Param("topic")
	consumerID := c.Query("consumerID")
	// TODO: Allow specifying partitionID in query later
	partitionID := 0
	batchQuery := c.DefaultQuery("batch", "1")

	if consumerID == "" {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Missing consumerID", "consumerID query parameter is required"))
		return
	}
	batchSize, err := strconv.Atoi(batchQuery)
	if err != nil || batchSize <= 0 {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid batch size", "Batch size must be a positive integer"))
		return
	}

	broker.mu.RLock()
	topic, topicExists := broker.Topics[topicName]
	broker.mu.RUnlock()
	if !topicExists {
		c.JSON(http.StatusNotFound, NewApiError("NotFoundError", "Topic not found", fmt.Sprintf("Topic '%s' does not exist", topicName)))
		return
	}

	topic.mu.RLock()
	partition, partitionExists := topic.Partitions[partitionID]
	topic.mu.RUnlock()
	if !partitionExists {
		// If the partition doesn't exist, the consumer's effective offset for it is 0.
		// No need to check the old global offsetStore.
		currentOffset := 0
		c.JSON(http.StatusOK, gin.H{"consumerID": consumerID, "topic": topicName, "partitionID": partitionID, "messages": []string{}, "nextOffset": currentOffset, "info": "partition not found or empty"})
		return
	}

	partition.mu.Lock() // Need Write lock to update offset
	defer partition.mu.Unlock()

	currentOffset := partition.ConsumerOffsets[consumerID] // Defaults to 0 if consumerID is new for this partition
	combinedMessages := partition.combineMessages()
	totalAvailable := len(combinedMessages)

	if currentOffset >= totalAvailable {
		c.JSON(http.StatusOK, gin.H{"consumerID": consumerID, "topic": topicName, "partitionID": partitionID, "messages": []string{}, "nextOffset": currentOffset, "info": "no new messages available for consumer"})
		return
	}

	endIndex := min(currentOffset+batchSize, totalAvailable)
	messagesToSend := combinedMessages[currentOffset:endIndex]
	nextOffset := endIndex

	// Update offset for this consumer *under lock*
	partition.ConsumerOffsets[consumerID] = nextOffset

	c.JSON(http.StatusOK, gin.H{
		"consumerID":  consumerID,
		"topic":       topicName,
		"partitionID": partitionID,
		"messages":    messagesToSend,
		"startOffset": currentOffset,
		"count":       len(messagesToSend),
		"nextOffset":  nextOffset,
	})
}

// handleGetTopics lists all available topic names.
func handleGetTopics(c *gin.Context) {
	broker.mu.RLock()
	defer broker.mu.RUnlock()

	topicNames := make([]string, 0, len(broker.Topics))
	for name := range broker.Topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames) // Return in alphabetical order

	c.JSON(http.StatusOK, gin.H{"topics": topicNames})
}

// PartitionInfo provides summary data for a partition.
type PartitionInfo struct {
	ID         int `json:"id"`
	HighCount  int `json:"highCount"`
	LowCount   int `json:"lowCount"`
	TotalCount int `json:"totalCount"`
	// Could add consumer offset info here later if needed
}

// handleGetPartitions lists partitions and their message counts for a given topic.
func handleGetPartitions(c *gin.Context) {
	topicName := c.Param("topic")

	broker.mu.RLock()
	topic, topicExists := broker.Topics[topicName]
	broker.mu.RUnlock()

	if !topicExists {
		c.JSON(http.StatusNotFound, NewApiError("NotFoundError", "Topic not found", fmt.Sprintf("Topic '%s' does not exist", topicName)))
		return
	}

	topic.mu.RLock() // Lock topic to safely iterate partitions map
	defer topic.mu.RUnlock()

	partitionInfos := make([]PartitionInfo, 0, len(topic.Partitions))
	partitionIDs := make([]int, 0, len(topic.Partitions))
	for id := range topic.Partitions {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Ints(partitionIDs) // Ensure consistent ordering

	for _, id := range partitionIDs {
		partition := topic.Partitions[id] // We know it exists as we're iterating keys
		partition.mu.RLock() // Lock individual partition to read counts
		highCount := len(partition.HighMessages)
		lowCount := len(partition.LowMessages)
		partition.mu.RUnlock()

		partitionInfos = append(partitionInfos, PartitionInfo{
			ID:         id,
			HighCount:  highCount,
			LowCount:   lowCount,
			TotalCount: highCount + lowCount,
		})
	}

	c.JSON(http.StatusOK, gin.H{"topic": topicName, "partitions": partitionInfos})
}