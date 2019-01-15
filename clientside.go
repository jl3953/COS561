package COS561Project

func write_only_txn(txn Transaction) {

	// 1. PREPARE PHASE.
	var waitgroup WaitGroup
	for key, value_to_be_written := range txn.data {

		waitgroup.Add(1)

		// concurrently send out all write operations as part of one transaction
		go func(data Data) {

			// try to write until it succeeds.
			succeeded_yet := false
			for !succeeded_yet {
				succeeded_yet = server.RPC_call("write_op", data)
			}

			waitgroup.Done()

		}(Data{key, value_to_be_written, unique_id, self})

	}

	wg.Wait() // block until all write operations have committed.

	// 2. COMMIT PHASE. When all writes return, concurrently send out
	// commit messages.
	for key, value_to_be_written := range txn.data_to_be_written {

		go func(data Data) {
			server.RPC_call("commit", data)
		}(Data{key, value_to_be_written, unique_id, self})
	}
}

func consistency_check(initial_read_results map[Key]ReadResult,
	final_read_results map[Key]Value) map[Key]int {

	for key, result := range initial_read_results {

		// check that the rest of the transaction exists
	}
}

func read_only_txn(txn Transaction) map[Key]Value {

	// 1. Round 1--perform initial reads of all keys.

	// both are declared as a primitive map to simplify pseudocode syntax, but could
	// be easily substituted using sync.Map to eliminate concurrent access
	// concerns
	var initial_read_results map[Key]ReadResult
	var final_read_results map[Key]Value

	for key, _ := range txn.data {

		go func(data Data) {

			value, writer_tid, other_ops := server.RPC_call("read_op", data)
			initial_read_results[key] = ReadResult{value, writer_tid, other_ops}

		}(Data{key, _, _, self})
	}

	// 2. Round 2--Perform a consistency check to ensure that all concurrent write
	// transactions completed. Fills in final_read_results with values that need
	// no further check and are already correct.
	further_checklist := consistency_check(initial_read_results, &final_read_results)

	// 3. Round 3--concurrently send out another round of reads to obtain correct values
	// for those keys that need it.
	for key, writer_tid := range further_checklist {

		go func(key Key, tid int) {
			value := server.RPC_call("read_previous", key, tid)
			final_read_results[key] = value
		}(key, writer_tid)
	}

	// 4. Round 4--concurrently release all locks that were acquired in round 1.
	for key, _ := range txn.data {

		go func(key Key) {
			server.RPC_call("release_lock", key)
		}(key)
	}
}
