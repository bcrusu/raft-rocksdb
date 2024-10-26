package raftrocksdb

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	flagLogStore    byte = 0
	flagStableStore byte = 8
)

//go:generate protoc --go_out=. --go_opt=paths=source_relative ./log.proto

// +---------+
// |   ID    |
// | 4 Bytes |
// +---------+
// Returns the key range for all store data, log and stable storage.
// RocksDB treats the end key as exclusive is the reason for the +1 below.
func encodeStoreKeyRange(id uint32) ([]byte, []byte) {
	startKey := make([]byte, 4)
	endKey := make([]byte, 4)

	binary.BigEndian.PutUint32(startKey, id)
	binary.BigEndian.PutUint32(endKey, id+1) // end key is exclusive
	return startKey, endKey
}

// +---------+--------+---------+
// |   ID    |  Flag  |  Index  |
// | 4 Bytes | 1 Byte | 8 Bytes |
// +---------+--------+---------+
func encodeLogKey(id uint32, index uint64) []byte {
	result := make([]byte, 0, 13)
	result = binary.BigEndian.AppendUint32(result, id)
	result = append(result, flagLogStore)
	result = binary.BigEndian.AppendUint64(result, index)
	return result
}

// Returns the key range [start, end+1) for log entries.
// RocksDB treats the end key as exclusive is the reason for the +1 below.
func encodeLogKeyRange(id uint32, start, end uint64) ([]byte, []byte) {
	startKey := encodeLogKey(id, start)

	if end < math.MaxUint64 {
		endKey := encodeLogKey(id, end+1)
		return startKey, endKey
	}

	endKey := encodeLogKey(id, 0)
	endKey[4]++
	return startKey, endKey
}

// Returns the key range for all log entries.
func encodeLogKeyFullRange(id uint32) ([]byte, []byte) {
	return encodeLogKeyRange(id, 0, math.MaxUint64)
}

func decodeLogKey(key []byte) (uint32, uint64) {
	if len(key) != 13 || key[4] != flagLogStore {
		panic(fmt.Sprintf("cannot decode invalid log store key=%s, length=%d", base64.RawURLEncoding.EncodeToString(key), len(key)))
	}

	id := binary.BigEndian.Uint32(key[:4])
	index := binary.BigEndian.Uint64(key[5:])
	return id, index
}

// +---------+--------+---------+
// |   ID    |  Flag  |   Key   |
// | 4 Bytes | 1 Byte | N Bytes |
// +---------+--------+---------+
func encodeStableKey(id uint32, key []byte) []byte {
	result := make([]byte, 0, len(key)+5)
	result = binary.BigEndian.AppendUint32(result, id)
	result = append(result, flagStableStore)
	result = append(result, key...)
	return result
}

func decodeStableKey(key []byte) (uint32, []byte) {
	if len(key) < 5 || key[4] != flagStableStore {
		panic(fmt.Sprintf("cannot decode invalid stable store key=%s, length=%d", base64.RawURLEncoding.EncodeToString(key), len(key)))
	}

	id := binary.BigEndian.Uint32(key[:4])
	return id, key[5:]
}

func encodeLog(log *raft.Log) ([]byte, error) {
	msg := &Log{
		Index:      log.Index,
		Term:       log.Term,
		Type:       encodeLogType(log.Type),
		Data:       log.Data,
		Extensions: log.Extensions,
		AppendedAt: timestamppb.New(log.AppendedAt),
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, wrap(err, "failed to marshal log proto")
	}

	return data, nil
}

func decodeLog(data []byte, log *raft.Log) error {
	msg := &Log{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}

	log.Index = msg.Index
	log.Term = msg.Term
	log.Type = decodeLogType(msg.Type)
	log.Data = slices.Clone(msg.Data)
	log.Extensions = slices.Clone(msg.Extensions)
	log.AppendedAt = msg.AppendedAt.AsTime()
	return nil
}

func encodeLogType(logType raft.LogType) LogType {
	switch logType {
	case raft.LogCommand:
		return LogType_LogCommand
	case raft.LogNoop:
		return LogType_LogNoop
	case raft.LogAddPeerDeprecated:
		return LogType_LogAddPeerDeprecated
	case raft.LogRemovePeerDeprecated:
		return LogType_LogRemovePeerDeprecated
	case raft.LogBarrier:
		return LogType_LogBarrier
	case raft.LogConfiguration:
		return LogType_LogConfiguration
	default:
		panic(fmt.Sprintf("unhandled raft log type %s", logType))
	}
}

func decodeLogType(logType LogType) raft.LogType {
	switch logType {
	case LogType_LogCommand:
		return raft.LogCommand
	case LogType_LogNoop:
		return raft.LogNoop
	case LogType_LogAddPeerDeprecated:
		return raft.LogAddPeerDeprecated
	case LogType_LogRemovePeerDeprecated:
		return raft.LogRemovePeerDeprecated
	case LogType_LogBarrier:
		return raft.LogBarrier
	case LogType_LogConfiguration:
		return raft.LogConfiguration
	default:
		panic(fmt.Sprintf("unhandled proto log type %s", logType))
	}
}

func encodeUint64(v uint64) []byte {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, v)
	return result
}

func decodeUint64(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid uint64 data=%s", base64.RawURLEncoding.EncodeToString(data))
	}

	return binary.LittleEndian.Uint64(data), nil
}
