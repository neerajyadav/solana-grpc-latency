package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/neerajyadav/proto"
)

type LatencyStats struct {
	mu            sync.RWMutex
	latencies     map[string][]uint64
	messageCount  map[string]uint64
	totalMessages uint64
	startTime     time.Time
}

func NewLatencyStats() *LatencyStats {
	return &LatencyStats{
		latencies:    make(map[string][]uint64),
		messageCount: make(map[string]uint64),
		startTime:    time.Now(),
	}
}

func (ls *LatencyStats) AddLatency(messageType string, latencyMs uint64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.latencies[messageType] = append(ls.latencies[messageType], latencyMs)
	ls.messageCount[messageType]++
	ls.totalMessages++
}

func (ls *LatencyStats) PrintStats() {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("LATENCY STATISTICS")
	fmt.Println(strings.Repeat("=", 80))

	totalDuration := time.Since(ls.startTime)
	fmt.Printf("Total Runtime: %v\n", totalDuration)
	fmt.Printf("Total Messages: %d\n", ls.totalMessages)
	fmt.Printf("Messages/sec: %.2f\n\n", float64(ls.totalMessages)/totalDuration.Seconds())

	for msgType, latencies := range ls.latencies {
		if len(latencies) == 0 {
			continue
		}

		fmt.Printf("Message Type: %s\n", msgType)
		fmt.Printf("  Count: %d\n", len(latencies))

		// Calculate statistics
		var sum, min, max uint64
		min = latencies[0]
		max = latencies[0]

		for _, lat := range latencies {
			sum += lat
			if lat < min {
				min = lat
			}
			if lat > max {
				max = lat
			}
		}

		avg := float64(sum) / float64(len(latencies))

		// Calculate percentiles
		sortedLatencies := make([]uint64, len(latencies))
		copy(sortedLatencies, latencies)
		sort.Slice(sortedLatencies, func(i, j int) bool {
			return sortedLatencies[i] < sortedLatencies[j]
		})

		p50 := sortedLatencies[len(sortedLatencies)*50/100]
		p95 := sortedLatencies[len(sortedLatencies)*95/100]
		p99 := sortedLatencies[len(sortedLatencies)*99/100]

		fmt.Printf("  Avg: %.2f ms\n", avg)
		fmt.Printf("  Min: %d ms\n", min)
		fmt.Printf("  Max: %d ms\n", max)
		fmt.Printf("  P50: %d ms\n", p50)
		fmt.Printf("  P95: %d ms\n", p95)
		fmt.Printf("  P99: %d ms\n", p99)
		fmt.Println()
	}

	fmt.Println(strings.Repeat("=", 80))
}

func main() {
	latencyStats := NewLatencyStats()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal, preparing to exit...")
		cancel()
	}()

	// Ensure stats are printed on exit
	defer func() {
		latencyStats.PrintStats()
	}()

	endpoint := "grpc.solanavibestation.com:10000"
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		// Increase max message sizes and optimize for performance
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(50*1024*1024), // 50MB
			grpc.MaxCallSendMsgSize(50*1024*1024), // 50MB
		),
		// Add connection pooling and performance optimizations
		grpc.WithReadBufferSize(1024 * 1024),  // 1MB read buffer
		grpc.WithWriteBufferSize(1024 * 1024), // 1MB write buffer
	}

	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		log.Fatal("Failed to connect to gRPC server:", err)
	}
	defer conn.Close()

	client := pb.NewGeyserClient(conn)

	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatal("Failed to create stream:", err)
	}

	filters := map[string]any{
		"transactions": map[string]any{
			// "client": map[string]any{
			// 	"vote":           false,
			// 	"failed":         false,
			// 	"accountInclude": []string{"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"},
			// },
		},
		"accounts": map[string]any{},
		"blocks": map[string]any{
			"client": map[string]any{
				"accountInclude":      []string{"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"},
				"includeAccounts":     true,
				"includeTransactions": true,
			},
		},
		"blocksMeta":         map[string]any{},
		"entry":              map[string]any{},
		"slots":              map[string]any{},
		"transactionsStatus": map[string]any{},
		"accountsDataSlice":  []any{},
	}

	request := CreateSubscribeRequest(pb.CommitmentLevel_CONFIRMED, filters)

	// Send the initial request
	if err := stream.Send(request); err != nil {
		log.Fatal("Failed to send subscribe request:", err)
	}

	log.Println("Started listening to gRPC stream...")

	// Message processing loop optimized for speed
	for {
		select {
		case <-ctx.Done():
			log.Println("Stream processing stopped due to context cancellation")
			return
		default:
			// Continue reading from the stream
		}

		// Receive message with minimal overhead
		update, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("Stream closed by server")
			} else {
				log.Printf("Error receiving from stream: %v", err)
			}
			return
		}

		// Process the update as fast as possible
		handleUpdate(update, latencyStats)
	}
}

func handleUpdate(update *pb.SubscribeUpdate, stats *LatencyStats) {
	// Use UTC for consistent timezone handling in latency calculations
	receptionTime := time.Now().UTC()
	createdAt := update.CreatedAt

	// Handle different types of updates and calculate latencies
	// The createdAt timestamp represents when the message was created at the server
	// Latency = time from server creation to local reception

	switch {
	case update.GetBlock() != nil:
		block := update.GetBlock()

		// For blocks, we have two potential timestamps:
		// 1. createdAt - when the server created this message
		// 2. block.BlockTime - when the block was actually produced on-chain

		if createdAt != nil {
			// Server-to-client latency
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("block_server_latency", latencyMs)
		}

		if block.BlockTime != nil && block.BlockTime.Timestamp > 0 {
			// On-chain to client latency (includes network propagation)
			// BlockTime is Unix timestamp in seconds, convert to UTC
			blockTime := time.Unix(block.BlockTime.Timestamp, 0).UTC()
			latencyMs := uint64(receptionTime.Sub(blockTime).Milliseconds())
			stats.AddLatency("block_onchain_latency", latencyMs)
		} else {
			stats.AddLatency("block_no_timestamp", 0)
		}

	case update.GetTransaction() != nil:
		// Calculate latency from when the message was created at the server
		// to when we received it locally
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("transaction", latencyMs)
		} else {
			stats.AddLatency("transaction_no_created_at", 0)
		}

	case update.GetAccount() != nil:
		// For accounts, use server creation time
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("account", latencyMs)
		} else {
			stats.AddLatency("account_no_created_at", 0)
		}

	case update.GetSlot() != nil:
		// For slots, use server creation time
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("slot", latencyMs)
		} else {
			stats.AddLatency("slot_no_created_at", 0)
		}

	case update.GetBlockMeta() != nil:
		blockMeta := update.GetBlockMeta()

		// Similar to blocks, we can measure both server and on-chain latency
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("block_meta_server_latency", latencyMs)
		}

		if blockMeta.BlockTime != nil && blockMeta.BlockTime.Timestamp > 0 {
			blockTime := time.Unix(blockMeta.BlockTime.Timestamp, 0).UTC()
			latencyMs := uint64(receptionTime.Sub(blockTime).Milliseconds())
			stats.AddLatency("block_meta_onchain_latency", latencyMs)
		} else {
			stats.AddLatency("block_meta_no_timestamp", 0)
		}

	case update.GetEntry() != nil:
		// For entries, use server creation time
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("entry", latencyMs)
		} else {
			stats.AddLatency("entry_no_created_at", 0)
		}

	case update.GetTransactionStatus() != nil:
		// For transaction status updates, use server creation time
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("transaction_status", latencyMs)
		} else {
			stats.AddLatency("transaction_status_no_created_at", 0)
		}

	default:
		if createdAt != nil {
			serverTime := createdAt.AsTime()
			latencyMs := uint64(receptionTime.Sub(serverTime).Milliseconds())
			stats.AddLatency("unknown", latencyMs)
		} else {
			stats.AddLatency("unknown_no_created_at", 0)
		}
	}
}

func CreateSubscribeRequest(commitment pb.CommitmentLevel, filters map[string]any) *pb.SubscribeRequest {
	req := &pb.SubscribeRequest{
		Commitment: &commitment,
	}

	// Add filters based on the map
	for filter, config := range filters {
		switch filter {
		case "blocks":
			// Skip if config is nil or empty
			configMap, ok := config.(map[string]any)
			if !ok || len(configMap) == 0 {
				continue
			}

			if req.Blocks == nil {
				req.Blocks = make(map[string]*pb.SubscribeRequestFilterBlocks)
			}

			// Process each block filter
			for blockKey, blockConfig := range configMap {
				blocksFilter := &pb.SubscribeRequestFilterBlocks{}
				hasData := false

				if blockConfigMap, ok := blockConfig.(map[string]any); ok {
					if includeAccounts, ok := blockConfigMap["includeAccounts"].(bool); ok {
						blocksFilter.IncludeAccounts = &includeAccounts
						hasData = true
					}
					if includeTransactions, ok := blockConfigMap["includeTransactions"].(bool); ok {
						blocksFilter.IncludeTransactions = &includeTransactions
						hasData = true
					}
					if includeEntries, ok := blockConfigMap["includeEntries"].(bool); ok {
						blocksFilter.IncludeEntries = &includeEntries
						hasData = true
					}
					if accounts, ok := blockConfigMap["accountInclude"].([]string); ok && len(accounts) > 0 {
						blocksFilter.AccountInclude = accounts
						hasData = true
					}
				}

				// Only add the filter if it has data
				if hasData {
					req.Blocks[blockKey] = blocksFilter
				}
			}

		case "transactions":
			// Skip if config is nil or empty
			configMap, ok := config.(map[string]any)
			if !ok || len(configMap) == 0 {
				continue
			}

			if req.Transactions == nil {
				req.Transactions = make(map[string]*pb.SubscribeRequestFilterTransactions)
			}

			// Process each transaction filter
			for txKey, txConfig := range configMap {
				txFilter := &pb.SubscribeRequestFilterTransactions{}
				hasData := false

				if txConfigMap, ok := txConfig.(map[string]any); ok {
					if vote, ok := txConfigMap["vote"].(bool); ok {
						txFilter.Vote = &vote
						hasData = true
					}
					if failed, ok := txConfigMap["failed"].(bool); ok {
						txFilter.Failed = &failed
						hasData = true
					}
					if signature, ok := txConfigMap["signature"].(string); ok && signature != "" {
						txFilter.Signature = &signature
						hasData = true
					}
					if accounts, ok := txConfigMap["accountInclude"].([]string); ok && len(accounts) > 0 {
						txFilter.AccountInclude = accounts
						hasData = true
					}
					if excludes, ok := txConfigMap["accountExclude"].([]string); ok && len(excludes) > 0 {
						txFilter.AccountExclude = excludes
						hasData = true
					}
					if required, ok := txConfigMap["accountRequired"].([]string); ok && len(required) > 0 {
						txFilter.AccountRequired = required
						hasData = true
					}
				}

				// Only add the filter if it has data
				if hasData {
					req.Transactions[txKey] = txFilter
				}
			}

		case "accounts":
			configMap, ok := config.(map[string]any)
			if !ok || len(configMap) == 0 {
				continue
			}

			accountsFilter := &pb.SubscribeRequestFilterAccounts{}
			hasData := false

			if accounts, ok := configMap["account"].([]string); ok && len(accounts) > 0 {
				accountsFilter.Account = accounts
				hasData = true
			}
			if owners, ok := configMap["owner"].([]string); ok && len(owners) > 0 {
				accountsFilter.Owner = owners
				hasData = true
			}

			// Only add accounts filter if it has data
			if hasData {
				if req.Accounts == nil {
					req.Accounts = make(map[string]*pb.SubscribeRequestFilterAccounts)
				}
				req.Accounts["accounts"] = accountsFilter
			}

		// Handle additional filter types only if they're not empty
		case "blocksMeta", "entry", "slots", "transactionsStatus":
			configMap, ok := config.(map[string]any)
			if !ok || len(configMap) == 0 {
				continue
			}

			// If we have specific fields to set for these filters in the future,
			// add the logic here and only include them if they have data

		case "accountsDataSlice":
			// Only process accountsDataSlice if it's a non-empty array
			if dataSlice, ok := config.([]any); !ok || len(dataSlice) == 0 {
				continue
			}
			// Process accountsDataSlice if needed
		}
	}

	return req
}
