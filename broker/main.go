// broker/main.go
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stathat/consistent"
)

const (
	DefaultNumPartitionsConstant = 3                   // Default partitions per topic if not specified
	MaxNumPartitions             = 256                 // Safety limit for number of partitions
	raftApplyTimeout             = 10 * time.Second    // Timeout for Raft Apply operations
	raftDataDirBase              = "falconq_raft_data" // Base directory for Raft logs (per node)
)

var broker *Broker // Global broker instance, initialized in main

// --- API Error Structure ---
type ApiError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func NewApiError(errType, message, details string) ApiError {
	return ApiError{Type: errType, Message: message, Details: details}
}

// --- Raft Command Structure ---
type RaftCommand struct {
	Operation string `json:"op"`        // e.g., "append"
	Topic     string `json:"topic"`     // Topic name
	Partition int    `json:"partition"` // Partition ID
	Offset    uint64 `json:"offset"`    // Offset assigned by the LEADER
	Value     string `json:"value"`     // Message content
	Priority  string `json:"priority"`  // Message priority
}

// --- Core Data Structures ---
type Message struct {
	Value        string  `json:"message"`
	Priority     string  `json:"priority"`
	PartitionKey *string `json:"partitionKey,omitempty"`
}
type StoredMessage struct {
	Value    string `json:"v"`
	Priority string `json:"p"`
	Offset   uint64 `json:"-"`
} // Offset for API, not stored in Badger value

type Partition struct {
	ID        int
	TopicName string
	db        *badger.DB       // For message data (state machine)
	offsetSeq *badger.Sequence // Message offset generator (ONLY USED BY LEADER)
	raftNode  *raft.Raft       // Raft instance for this partition
	raftFSM   *partitionFSM    // FSM for this partition
	// TODO: Add retention policy logic later
}

type Topic struct {
	Name              string
	Partitions        map[int]*Partition
	mu                sync.RWMutex // Protects Partitions map & roundRobinCounter
	db                *badger.DB
	Config            *Config                // Reference to global config for peer info
	NodeID            string                 // Current node's ID
	RaftTransport     *raft.NetworkTransport // Shared Raft transport for this node
	NumPartitions     int
	HashRing          *consistent.Consistent
	roundRobinCounter uint64
}

type Broker struct {
	Topics        map[string]*Topic
	mu            sync.RWMutex
	db            *badger.DB
	Config        *Config                // Store global config here
	NodeID        string                 // Current node's ID
	RaftTransport *raft.NetworkTransport // Shared Raft transport for this node
}

// --- Raft FSM (Finite State Machine) ---
type partitionFSM struct {
	db          *badger.DB // BadgerDB for this partition's messages
	topicName   string
	partitionID int
}

func (fsm *partitionFSM) Apply(rlog *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(rlog.Data, &cmd); err != nil {
		log.Printf("FATAL: FSM Apply: Unmarshal Raft log for %s p%d: %v. Data: %s", fsm.topicName, fsm.partitionID, err, string(rlog.Data))
		return fmt.Errorf("unmarshal RaftCommand: %w", err) // Return error for Raft to handle
	}
	if cmd.Operation != "append" {
		log.Printf("WARN: FSM Apply: Unknown op '%s' for %s p%d", cmd.Operation, fsm.topicName, fsm.partitionID)
		return fmt.Errorf("unknown op: %s", cmd.Operation)
	}
	key := GenerateMessageKey(cmd.Topic, cmd.Partition, cmd.Offset)
	storedMsgData := StoredMessage{Value: cmd.Value, Priority: cmd.Priority} // cmd.Offset not stored in value
	valueBytes, err := json.Marshal(storedMsgData)
	if err != nil {
		log.Printf("FATAL: FSM Apply: Marshal StoredMessage for %s p%d offset %d: %v", cmd.Topic, cmd.Partition, cmd.Offset, err)
		return fmt.Errorf("marshal StoredMessage: %w", err)
	}
	err = fsm.db.Update(func(txn *badger.Txn) error { return txn.Set(key, valueBytes) })
	if err != nil {
		log.Printf("FATAL: FSM Apply: Set key %s in BadgerDB for %s p%d: %v", key, cmd.Topic, cmd.Partition, err)
		return fmt.Errorf("FSM apply Set: %w", err)
	}
	return nil
}
func (fsm *partitionFSM) Snapshot() (raft.FSMSnapshot, error) { return &dummySnapshot{}, nil }
func (fsm *partitionFSM) Restore(rc io.ReadCloser) error {
	log.Printf("WARN: FSM Restore called for %s p%d (not implemented).", fsm.topicName, fsm.partitionID)
	return rc.Close()
}

type dummySnapshot struct{}

func (s *dummySnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (s *dummySnapshot) Release()                             {}

// --- Initialization ---
func NewBroker(db *badger.DB, cfg *Config, transport *raft.NetworkTransport) *Broker {
	return &Broker{Topics: make(map[string]*Topic), db: db, Config: cfg, NodeID: cfg.CurrentNodeID, RaftTransport: transport}
}
func NewTopic(name string, db *badger.DB, numPartitions int, cfg *Config, nodeID string, transport *raft.NetworkTransport) (*Topic, error) {
	if numPartitions <= 0 {
		numPartitions = cfg.DefaultNumPartitions
	}
	if numPartitions > MaxNumPartitions {
		return nil, fmt.Errorf("partitions %d > max %d", numPartitions, MaxNumPartitions)
	}
	members := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		members[i] = strconv.Itoa(i)
	}
	hashRing := consistent.New()
	hashRing.NumberOfReplicas = 20
	hashRing.Set(members)
	return &Topic{Name: name, Partitions: make(map[int]*Partition), mu: sync.RWMutex{}, db: db, Config: cfg, NodeID: nodeID, RaftTransport: transport, NumPartitions: numPartitions, HashRing: hashRing}, nil
}
func InitializePartition(id int, topicName string, db *badger.DB, cfg *Config, transport *raft.NetworkTransport) (*Partition, error) {
	msgSeqKey := []byte(fmt.Sprintf("t/%s/p/%d/_msg_seq", topicName, id))
	msgSeq, err := db.GetSequence(msgSeqKey, 100)
	if err != nil {
		return nil, fmt.Errorf("get msg sequence for %s p%d: %w", topicName, id, err)
	}
	p := &Partition{ID: id, TopicName: topicName, db: db, offsetSeq: msgSeq}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(fmt.Sprintf("%s:%s_p%d", cfg.CurrentNodeID, topicName, id))
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("raft-%s-P%d-%s", topicName, id, cfg.CurrentNodeID),
		Output: io.Discard, // Use os.Stdout for debugging Raft core logs
		Level:  hclog.Error,
	})

	partitionRaftDir := filepath.Join(raftDataDirBase, cfg.CurrentNodeID, topicName, fmt.Sprintf("p%d", id))
	if err := os.MkdirAll(partitionRaftDir, 0700); err != nil {
		return nil, fmt.Errorf("create Raft dir %s: %w", partitionRaftDir, err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(partitionRaftDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("NewBoltStore(log) %s p%d: %w", topicName, id, err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(partitionRaftDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("NewBoltStore(stable) %s p%d: %w", topicName, id, err)
	}
	snapshotStore := raft.NewInmemSnapshotStore()
	fsm := &partitionFSM{db: db, topicName: topicName, partitionID: id}
	p.raftFSM = fsm
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("NewRaft for %s p%d: %w", topicName, id, err)
	}
	p.raftNode = raftNode

	var raftServers []raft.Server
	for peerNodeID, peerNodeConfig := range cfg.Nodes {
		peerRaftID := raft.ServerID(fmt.Sprintf("%s:%s_p%d", peerNodeID, topicName, id))
		raftServers = append(raftServers, raft.Server{ID: peerRaftID, Address: raft.ServerAddress(peerNodeConfig.RaftAddress), Suffrage: raft.Voter})
	}
	if len(raftServers) == 0 {
		log.Printf("WARN: No peers in config for Raft group %s p%d. Standalone mode.", topicName, id)
		configuration := raft.Configuration{Servers: []raft.Server{{ID: raftConfig.LocalID, Address: transport.LocalAddr(), Suffrage: raft.Voter}}}
		sf := p.raftNode.BootstrapCluster(configuration)
		if err := sf.Error(); err != nil {
			log.Printf("WARN: Bootstrap single-node Raft %s p%d: %v", topicName, id, err)
		}
	} else {
		hasState, _ := raft.HasExistingState(logStore, stableStore, snapshotStore)
		var sortedNodeIDs []string
		for nodeID := range cfg.Nodes {
			sortedNodeIDs = append(sortedNodeIDs, nodeID)
		}
		sort.Strings(sortedNodeIDs)
		isDesignatedBootstrapper := len(sortedNodeIDs) > 0 && sortedNodeIDs[0] == cfg.CurrentNodeID
		if !hasState && isDesignatedBootstrapper {
			log.Printf("Node %s bootstrapping Raft for %s p%d with servers: %+v", cfg.CurrentNodeID, topicName, id, raftServers)
			configuration := raft.Configuration{Servers: raftServers}
			f := p.raftNode.BootstrapCluster(configuration)
			if err := f.Error(); err != nil {
				log.Printf("WARN: Bootstrap Raft for %s p%d: %v", topicName, id, err)
			}
		} else if hasState {
			log.Printf("Found existing Raft state for %s p%d on %s", topicName, id, cfg.CurrentNodeID)
		} else {
			log.Printf("Node %s for %s p%d not bootstrapper; awaiting join/leadership.", cfg.CurrentNodeID, topicName, id)
		}
	}
	return p, nil
}

// --- Broker/Topic Methods ---
func (b *Broker) getOrCreateTopic(topicName string) (*Topic, error) {
	b.mu.RLock()
	topic, exists := b.Topics[topicName]
	b.mu.RUnlock()
	if exists {
		return topic, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	topic, exists = b.Topics[topicName]
	if exists {
		return topic, nil
	}
	var err error
	topic, err = NewTopic(topicName, b.db, b.Config.DefaultNumPartitions, b.Config, b.NodeID, b.RaftTransport)
	if err != nil {
		return nil, fmt.Errorf("create topic %s: %w", topicName, err)
	}
	log.Printf("✨ Topic '%s' created with %d partitions.", topicName, topic.NumPartitions)
	b.Topics[topicName] = topic
	for i := 0; i < topic.NumPartitions; i++ {
		if _, errInit := topic.getOrCreatePartition(i); errInit != nil {
			log.Printf("ERROR: Eager init partition %d for %s: %v", i, topicName, errInit)
		}
	}
	return topic, nil
}
func (t *Topic) getOrCreatePartition(partitionID int) (*Partition, error) {
	if partitionID < 0 || partitionID >= t.NumPartitions {
		return nil, fmt.Errorf("invalid partition ID %d for %s (max %d)", partitionID, t.Name, t.NumPartitions-1)
	}
	t.mu.RLock()
	partition, exists := t.Partitions[partitionID]
	t.mu.RUnlock()
	if exists {
		return partition, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	partition, exists = t.Partitions[partitionID]
	if exists {
		return partition, nil
	}
	var err error
	partition, err = InitializePartition(partitionID, t.Name, t.db, t.Config, t.RaftTransport)
	if err != nil {
		log.Printf("❌ Failed init partition %d for %s: %v", partitionID, t.Name, err)
		return nil, err
	}
	log.Printf("🔧 Partition %d for topic '%s' initialized with Raft.", partitionID, t.Name)
	t.Partitions[partitionID] = partition
	return partition, nil
}
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	log.Printf("Releasing resources for %d topics...", len(b.Topics))
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
					log.Printf("WARN: Release msg sequence %s p%d: %v", topic.Name, partition.ID, err)
				}
			}
			if partition.raftNode != nil {
				sf := partition.raftNode.Shutdown()
				if err := sf.Error(); err != nil {
					log.Printf("WARN: Shutdown Raft %s p%d: %v", topic.Name, partition.ID, err)
				}
			}
		}
	}
	log.Println("Closing BadgerDB...")
	return b.db.Close()
}

// --- Partition Methods (Key Generators, Persistence, Offset Management) ---
func GenerateMessageKey(topicName string, partitionID int, offset uint64) []byte {
	keyPrefix := fmt.Sprintf("t/%s/p/%d/m/", topicName, partitionID)
	key := make([]byte, len(keyPrefix)+8)
	copy(key, []byte(keyPrefix))
	binary.BigEndian.PutUint64(key[len(keyPrefix):], offset)
	return key
}
func GenerateConsumerOffsetKey(topicName string, partitionID int, consumerID string) []byte {
	return []byte(fmt.Sprintf("t/%s/p/%d/c/%s", topicName, partitionID, consumerID))
}
func GetOffsetFromMessageKey(key []byte, prefixLen int) (uint64, error) {
	if len(key) != prefixLen+8 {
		return 0, fmt.Errorf("invalid key len: got %d, want %d", len(key), prefixLen+8)
	}
	return binary.BigEndian.Uint64(key[prefixLen:]), nil
}
func (p *Partition) GetNextMessageOffset() (uint64, error) {
	if p.offsetSeq == nil {
		return 0, fmt.Errorf("msg sequence not init for p%d", p.ID)
	}
	return p.offsetSeq.Next()
}
func (p *Partition) ReadMessages(startOffset uint64, batchSize int) ([]StoredMessage, uint64, error) {
	messages := make([]StoredMessage, 0, batchSize)
	var nextOffset uint64 = startOffset
	err := p.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = batchSize * 2
		it := txn.NewIterator(opts)
		defer it.Close()
		keyPrefixBytes := []byte(fmt.Sprintf("t/%s/p/%d/m/", p.TopicName, p.ID))
		startKey := GenerateMessageKey(p.TopicName, p.ID, startOffset)
		lastProcessedOffset := startOffset - 1
		it.Seek(startKey)
		for ; it.ValidForPrefix(keyPrefixBytes) && len(messages) < batchSize; it.Next() {
			item := it.Item()
			key := item.Key()
			currentKeyOffset, err := GetOffsetFromMessageKey(key, len(keyPrefixBytes))
			if err != nil {
				log.Printf("WARN: Skip key fmt p%d: %s (%v)", p.ID, key, err)
				continue
			}
			var msg StoredMessage
			err = item.Value(func(val []byte) error {
				if err := json.Unmarshal(val, &msg); err != nil {
					log.Printf("WARN: Unmarshal msg p%d offset %d: %v", p.ID, currentKeyOffset, err)
					return nil
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("read val p%d offset %d: %w", p.ID, currentKeyOffset, err)
			}
			if msg.Value == "" && msg.Priority == "" {
				continue
			}
			msg.Offset = currentKeyOffset
			messages = append(messages, msg)
			lastProcessedOffset = currentKeyOffset
		}
		nextOffset = lastProcessedOffset + 1
		return nil
	})
	if err != nil {
		return nil, startOffset, fmt.Errorf("batch read p%d: %w", p.ID, err)
	}
	return messages, nextOffset, nil
}
func (p *Partition) GetConsumerOffset(consumerID string) (uint64, error) {
	key := GenerateConsumerOffsetKey(p.TopicName, p.ID, consumerID)
	var offset uint64 = 0
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		err = item.Value(func(val []byte) error {
			if len(val) != 8 {
				log.Printf("ERR: Corrupted offset consumer '%s' %s p%d. Len: %d. Reset 0.", consumerID, p.TopicName, p.ID, len(val))
				offset = 0
				return nil
			}
			offset = binary.BigEndian.Uint64(val)
			return nil
		})
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("get consumer offset '%s' on '%s'/p%d: %w", consumerID, p.TopicName, p.ID, err)
	}
	return offset, nil
}
func (p *Partition) SetConsumerOffset(consumerID string, offset uint64) error {
	key := GenerateConsumerOffsetKey(p.TopicName, p.ID, consumerID)
	valBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valBytes, offset)
	err := p.db.Update(func(txn *badger.Txn) error { return txn.Set(key, valBytes) })
	if err != nil {
		return fmt.Errorf("set consumer offset '%s' on '%s'/p%d to %d: %w", consumerID, p.TopicName, p.ID, offset, err)
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
func getAdvertiseHost(cfg *Config) string {
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, i := range ifaces {
			addrs, err := i.Addrs()
			if err == nil {
				for _, addr := range addrs {
					var ip net.IP
					switch v := addr.(type) {
					case *net.IPNet:
						ip = v.IP
					case *net.IPAddr:
						ip = v.IP
					}
					if ip != nil && !ip.IsLoopback() && ip.To4() != nil {
						return ip.String()
					}
				}
			}
		}
	}
	if cfg.CurrentNode != nil {
		host, _, _ := net.SplitHostPort(cfg.CurrentNode.HTTPAddress)
		if host != "" && host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return host
		}
	}
	return "127.0.0.1"
}

// --- Config Structs and Loading ---
type NodeConfig struct {
	HTTPAddress string `yaml:"http_address"`
	RaftAddress string `yaml:"raft_address"`
}
type Config struct {
	DefaultNumPartitions int                   `yaml:"default_num_partitions"`
	Nodes                map[string]NodeConfig `yaml:"nodes"`
	CurrentNodeID        string                `yaml:"-"`
	CurrentNode          *NodeConfig           `yaml:"-"`
}

func LoadConfig(path string, currentNodeID string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cfg %s: %w", path, err)
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("unmarshal cfg %s: %w", path, err)
	}
	cfg.CurrentNodeID = currentNodeID
	nodeSpecificConfig, ok := cfg.Nodes[currentNodeID]
	if !ok {
		return nil, fmt.Errorf("node_id '%s' not in cfg nodes", currentNodeID)
	}
	cfg.CurrentNode = &nodeSpecificConfig
	if cfg.CurrentNode.HTTPAddress == "" {
		return nil, fmt.Errorf("http_address missing for node %s", currentNodeID)
	}
	if cfg.CurrentNode.RaftAddress == "" {
		return nil, fmt.Errorf("raft_address missing for node %s", currentNodeID)
	}
	if cfg.DefaultNumPartitions == 0 {
		cfg.DefaultNumPartitions = DefaultNumPartitionsConstant
	}
	return &cfg, nil
}
func (cfg *Config) GetPeerHTTPAddress(raftNodeAddress string) string { // raftNodeAddress is like "host:raft_port"
	targetRaftHost, targetRaftPortStr, err := net.SplitHostPort(raftNodeAddress)
	if err != nil {
		log.Printf("ERROR: Invalid Raft node address format for lookup: %s", raftNodeAddress)
		return ""
	}
	targetRaftPort, _ := strconv.Atoi(targetRaftPortStr)
	for _, nodeCfg := range cfg.Nodes {
		nodeRaftHost, nodeRaftPortStr, _ := net.SplitHostPort(nodeCfg.RaftAddress)
		nodeRaftPort, _ := strconv.Atoi(nodeRaftPortStr)
		isHostMatch := (nodeRaftHost == targetRaftHost) || (targetRaftHost == "127.0.0.1" && (nodeRaftHost == "localhost" || nodeRaftHost == "")) || (targetRaftHost == "localhost" && (nodeRaftHost == "127.0.0.1" || nodeRaftHost == ""))
		if isHostMatch && nodeRaftPort == targetRaftPort {
			return fmt.Sprintf("http://%s", nodeCfg.HTTPAddress)
		}
	}
	log.Printf("WARN: Could not find peer config for Raft address %s", raftNodeAddress)
	return ""
}

// --- Main Function ---
func main() {
	fmt.Println("Starting FalconQ Broker...")
	configPath := flag.String("config", "config.yaml", "Path to config file")
	nodeIDFlag := flag.String("nodeid", "", "This node's ID (must match a key in config.yaml nodes section)")
	flag.Parse()
	if *nodeIDFlag == "" {
		log.Fatal("-nodeid flag is required")
	}
	cfg, err := LoadConfig(*configPath, *nodeIDFlag)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	raftBindAddr := cfg.CurrentNode.RaftAddress
	raftAdvertiseHost := getAdvertiseHost(cfg)
	_, raftAdvertisePortStr, _ := net.SplitHostPort(cfg.CurrentNode.RaftAddress)
	raftAdvertiseAddrFull := fmt.Sprintf("%s:%s", raftAdvertiseHost, raftAdvertisePortStr)
	raftAdvertiseNetAddr, err := net.ResolveTCPAddr("tcp", raftAdvertiseAddrFull)
	if err != nil {
		log.Fatalf("Resolve Raft advertise address %s: %v", raftAdvertiseAddrFull, err)
	}
	transport, err := raft.NewTCPTransport(raftBindAddr, raftAdvertiseNetAddr, 3, 10*time.Second, io.Discard)
	if err != nil {
		log.Fatalf("Create Raft transport: %v", err)
	}

	dbPath := fmt.Sprintf("falconq_data_%s", cfg.CurrentNodeID)
	_, httpPortStr, _ := net.SplitHostPort(cfg.CurrentNode.HTTPAddress)
	port := fmt.Sprintf(":%s", httpPortStr)
	fmt.Printf("Node ID: %s | HTTP on: %s | Raft on: %s (Advertise: %s)\n", cfg.CurrentNodeID, cfg.CurrentNode.HTTPAddress, cfg.CurrentNode.RaftAddress, raftAdvertiseAddrFull)
	fmt.Printf("Opening BadgerDB at path: %s\n", dbPath)
	opts := badger.DefaultOptions(dbPath).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("❌ Open BadgerDB: %v", err)
	}
	broker = NewBroker(db, cfg, transport)
	fmt.Println("Broker initialized.")
	if _, err := broker.getOrCreateTopic("default-topic"); err != nil {
		log.Printf("Error pre-creating default topic: %v", err)
	} // Eagerly create a topic to start Raft groups

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())
	r.POST("/topic/:topic/publish", handlePublish)
	r.GET("/topic/:topic/peek", handlePeek)
	r.GET("/topic/:topic/consume", handleConsume)
	r.GET("/topics", handleGetTopics)
	r.GET("/topics/:topic/partitions", handleGetPartitions)
	r.GET("/raft/stats", handleRaftStats)
	srv := &http.Server{Addr: port, Handler: r}
	go func() {
		fmt.Printf("🚀 FalconQ Broker API for node %s running at http://%s | Raft on %s\n", cfg.CurrentNodeID, cfg.CurrentNode.HTTPAddress, cfg.CurrentNode.RaftAddress)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("🚦 Shutting down server for node %s...", cfg.CurrentNodeID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("❌ Server %s forced to shutdown: %v", cfg.CurrentNodeID, err)
	}
	log.Printf("🧹 Closing broker resources for node %s...", cfg.CurrentNodeID)
	if broker != nil {
		if err := broker.Close(); err != nil {
			log.Printf("❌ Error closing broker resources for %s: %v", cfg.CurrentNodeID, err)
		} else {
			log.Printf("✅ Broker resources for %s closed.", cfg.CurrentNodeID)
		}
	} else if db != nil {
		db.Close()
	}
	log.Printf("👋 Server %s exiting", cfg.CurrentNodeID)
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
	var targetPartitionID int
	if msg.PartitionKey != nil && *msg.PartitionKey != "" {
		partitionStr, err := topic.HashRing.Get(*msg.PartitionKey)
		if err != nil {
			log.Printf("WARN: Consistent hash topic %s key '%s': %v. Fallback.", topicName, *msg.PartitionKey, err)
			topic.mu.Lock()
			targetPartitionID = int(topic.roundRobinCounter % uint64(topic.NumPartitions))
			topic.roundRobinCounter++
			topic.mu.Unlock()
		} else {
			targetPartitionID, err = strconv.Atoi(partitionStr)
			if err != nil {
				log.Printf("WARN: Consistent hash non-int '%s' topic %s key '%s': %v. Fallback.", partitionStr, topicName, *msg.PartitionKey, err)
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
	if partition.raftNode == nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Raft not initialized for partition", fmt.Sprintf("p%d", targetPartitionID)))
		return
	}

	if partition.raftNode.State() != raft.Leader {
		leaderRaftAddr := string(partition.raftNode.Leader())
		if leaderRaftAddr == "" {
			c.JSON(http.StatusServiceUnavailable, NewApiError("ServiceUnavailable", "No leader for partition", fmt.Sprintf("p%d", targetPartitionID)))
			return
		}
		leaderHTTPAddr := broker.Config.GetPeerHTTPAddress(leaderRaftAddr) // Use broker.Config
		if leaderHTTPAddr == "" {
			c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Could not resolve leader HTTP address", leaderRaftAddr))
			return
		}
		log.Printf("Forwarding publish for topic %s p%d to leader %s (%s)", topicName, targetPartitionID, leaderRaftAddr, leaderHTTPAddr)
		forwardURL := fmt.Sprintf("%s%s", leaderHTTPAddr, c.Request.URL.Path)
		if c.Request.URL.RawQuery != "" {
			forwardURL += "?" + c.Request.URL.RawQuery
		}
		bodyBytes, _ := json.Marshal(msg)
		proxyReq, err := http.NewRequest(c.Request.Method, forwardURL, bytes.NewBuffer(bodyBytes))
		if err != nil {
			c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to create forward request", err.Error()))
			return
		}
		proxyReq.Header = c.Request.Header.Clone()
		proxyReq.Header.Set("Content-Type", "application/json")
		httpClient := &http.Client{Timeout: 5 * time.Second}
		resp, err := httpClient.Do(proxyReq)
		if err != nil {
			c.JSON(http.StatusBadGateway, NewApiError("BadGateway", "Failed to forward request to leader", err.Error()))
			return
		}
		defer resp.Body.Close()
		for k, vv := range resp.Header {
			for _, v := range vv {
				c.Writer.Header().Add(k, v)
			}
		}
		c.Writer.WriteHeader(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
		return
	}
	assignedOffset, err := partition.GetNextMessageOffset()
	if err != nil {
		log.Printf("❌ Leader failed to get next offset for topic '%s' p%d: %v", topicName, targetPartitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to generate offset", err.Error()))
		return
	}
	cmd := RaftCommand{Operation: "append", Topic: topicName, Partition: targetPartitionID, Offset: assignedOffset, Value: msg.Value, Priority: msg.Priority}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to marshal Raft command", err.Error()))
		return
	}
	applyFuture := partition.raftNode.Apply(cmdBytes, raftApplyTimeout)
	if err := applyFuture.Error(); err != nil {
		log.Printf("❌ Raft Apply failed for topic '%s' p%d: %v", topicName, targetPartitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to commit message via Raft", err.Error()))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "message committed", "topic": topicName, "partitionID": targetPartitionID, "offset": assignedOffset, "priority": msg.Priority})
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
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Missing or invalid partitionID", "partitionID query parameter is required"))
		return
	}
	topic, err := broker.getOrCreateTopic(topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access topic", err.Error()))
		return
	}
	if partitionID >= topic.NumPartitions {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid partitionID", fmt.Sprintf("Partition ID must be < %d for topic %s", topic.NumPartitions, topicName)))
		return
	}
	partition, err := topic.getOrCreatePartition(partitionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access specified partition", err.Error()))
		return
	}
	rawMessages, nextOffsetRead, err := partition.ReadMessages(startOffset, batchSize)
	if err != nil {
		log.Printf("❌ Read messages peek topic '%s' p%d: %v", topicName, partitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to read messages", err.Error()))
		return
	}
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
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Missing or invalid partitionID", "partitionID query parameter is required"))
		return
	}
	topic, err := broker.getOrCreateTopic(topicName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access topic", err.Error()))
		return
	}
	if partitionID >= topic.NumPartitions {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid partitionID", fmt.Sprintf("Partition ID must be < %d for topic %s", topic.NumPartitions, topicName)))
		return
	}
	partition, err := topic.getOrCreatePartition(partitionID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to access specified partition", err.Error()))
		return
	}
	currentConsumerOffset, err := partition.GetConsumerOffset(consumerID)
	if err != nil {
		log.Printf("❌ Get consumer offset consumer '%s' topic '%s' p%d: %v", consumerID, topicName, partitionID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to retrieve consumer offset", err.Error()))
		return
	}
	rawMessages, nextOffsetRead, err := partition.ReadMessages(currentConsumerOffset, batchSize)
	if err != nil {
		log.Printf("❌ Read messages consume topic '%s' p%d consumer '%s': %v", topicName, partitionID, consumerID, err)
		c.JSON(http.StatusInternalServerError, NewApiError("ServerError", "Failed to read messages", err.Error()))
		return
	}
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
	var finalConsumerNextOffset uint64
	if len(messagesToSend) > 0 {
		finalConsumerNextOffset = messagesToSend[len(messagesToSend)-1].Offset + 1
	} else {
		finalConsumerNextOffset = nextOffsetRead
	}
	err = partition.SetConsumerOffset(consumerID, finalConsumerNextOffset)
	if err != nil {
		log.Printf("ERROR: Set consumer offset consumer '%s' topic '%s' p%d to %d: %v", consumerID, topicName, partitionID, finalConsumerNextOffset, err)
	}
	c.JSON(http.StatusOK, gin.H{"consumerID": consumerID, "topic": topicName, "partitionID": partitionID, "messages": messagesToSend, "startOffset": currentConsumerOffset, "count": len(messagesToSend), "nextOffset": finalConsumerNextOffset})
}
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
	} // Only list initialized partitions
	sort.Ints(partitionIDs)
	for _, id := range partitionIDs {
		partitionInfos = append(partitionInfos, PartitionInfo{ID: id})
	}
	c.JSON(http.StatusOK, gin.H{"topic": topicName, "configuredPartitions": topic.NumPartitions, "partitionKeyStrategy": "consistent_hash+round_robin_fallback", "activePartitions": partitionInfos})
}
func handleRaftStats(c *gin.Context) {
	topicName := c.Query("topic")
	partitionIDStr := c.Query("partitionID")
	if topicName == "" || partitionIDStr == "" {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "topic and partitionID query parameters are required", ""))
		return
	}
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, NewApiError("ValidationError", "Invalid partitionID format", err.Error()))
		return
	}
	broker.mu.RLock()
	topic, topicExists := broker.Topics[topicName]
	broker.mu.RUnlock()
	if !topicExists {
		c.JSON(http.StatusNotFound, NewApiError("NotFoundError", "Topic not found", topicName))
		return
	}
	topic.mu.RLock()
	partition, partExists := topic.Partitions[partitionID]
	topic.mu.RUnlock()
	if !partExists {
		c.JSON(http.StatusNotFound, NewApiError("NotFoundError", "Partition not found or not yet initialized", fmt.Sprintf("%s/p%d", topicName, partitionID)))
		return
	}
	if partition.raftNode == nil {
		c.JSON(http.StatusServiceUnavailable, NewApiError("ServiceUnavailable", "Raft not initialized for this partition", ""))
		return
	}
	c.JSON(http.StatusOK, partition.raftNode.Stats())
}
