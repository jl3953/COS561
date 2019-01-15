package COS561Project

const (
	WRITE_SUCCESS   = "Write Success"
	LOCK_WAIT_ABORT = "Lock Wait Abort"
)

type Key string   // could be any data type, let's say string for simplicity
type Value string // could be any data type

type ReadResult struct {
	value         Value
	writer_tid    int
	other_txn_ops map[Key]Value
}

// Assumptions:
// 1) There exists a type of lock (most similar to a RW lock) that behaves as follows:
// - An indefinite number of readers can simultaneously acquire the lock.
// - Only a single writer at a time can acquire the lock.
// - When readers hold the lock, no writer can acquire the lock.
// - When a writer holds the lock, the reader is still allowed to acquire the lock,
// and the writer is expected to keep the lock until it finishes. However, the
// reader acquiring the lock prevents other writers from acquiring the same lock
// once the read starts.
// 2) Writes do not fail.
//
// Writer on server side, upon receiving a write request (implements 2PC):
// 1) Attempt to acquire lock on the data that it needs (lock granularity will
// depend upon the data being stored). Abort upon failure.
// 2) Now that lock has been acquired, write value.
// 3) Metadata: write unique transaction id (generated the same way RIFL does).
// 4) Metadata: write the other values being modified in the transaction.
func (writer *server_side_writer) write_op(data Data) bool {

	// 1. PREPARE PHASE

	// lock granularity corresponds to the data being stored and is a property
	// of the underlying system. Non-blocking call that returns false if the lock cannot
	// be acquired.
	// The requirement that writes acquire a RW lock prevents the write from
	// starting after a read on the same value(s) has started.
	if !acquire_lock_as_writer(data.key) {
		return false, LOCK_WAIT_ABORT
	}

	// write the value
	writer.write_value(data.key, data.value)

	// write metadata, which includes 1) unique_transaction_id, and 2) remaining
	// write operations part of the same transaction
	writer.write_metadata("writer_unique_tid", data.key, data.unique_transaction_id)
	for _, other_write_op := range data.remaining_transaction_ops {
		writer.write_metadata("writer_other_txn_ops", data.key, other_write_op)
	}

	// acknowledge to client that the write succeeded.
	return true, WRITE_SUCCESS
}

func (writer *server_side_writer) commit(data Data) {

	// 2. COMMIT PHASE

	// block until receiving client's response. Upon receiving client's response,
	// writer knows that the client has fanned out all commit messages.
	// Assume writes do not fail.
	release_lock(data.key)
}

// Reader on server side, upon receiving read request:
// 1) Acquire lock to prevent writes from starting during its read. Writes that
// started beforehand are allowed to finish.
// 2) Read value.
// 3) Read associated metadata.
func (reader *server_side_reader) read_op(data Data) (Value, int, []Key) {

	// acquire lock and defer its release until the end of the read operation,
	// will always be successful.
	// The acquisition of the RW lock prevents writes from starting after
	// a read has begun.
	acquire_lock_as_reader(data.key)

	// read value
	value := reader.read(data.key)

	// read metadata
	writer_transaction_id := reader.read_metadata("writer_unique_tid", data.key)
	write_txn_other_ops := reader.read_metadata("writer_other_txn_ops", data.key)

	// send data and associated metadata back to client.
	return value, writer_transaction_id, write_txn_other_ops
}

func (reader *server_side_reader) read_history(key Key, writer_unique_tid int) (bool, Value) {

	// Note: no need to acquire lock to read a key's history of writes,
	// b/c it is append-only.
	var exists bool // does the tid for specified key exist
	var value Value // if tid exists for key, what is the associated value

	exists, value = reader.read_by_tid(key, writer_unique_tid)

	if exists {
		return true, value
	} else {
		return false, nil
	}
}

func (reader *server_side_reader) read_previous(key Key, writer_unique_tid int) Value {

	_, value := reader.read_previous_tid(key, writer_unique_tid)
	return value
}

func (reader *server_side_reader) release_lock(key Key) {
	release_lock(data.key)
}
