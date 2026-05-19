2026 May 18


Quick recap
Rui and Roman discussed implementing a new approach for checkpointing and snapshotting data during task recovery. They explored two main directions: reusing the existing task state snapshot format versus introducing a new format specifically designed for partial snapshots. Rui explained the benefits of using control messages as barriers between data buffers to avoid making extensive changes to input channels, and they discussed how to handle sequential I/O operations during checkpointing. Roman proposed a three-step approach where the spinning thread would first notify the task thread about checkpoint requirements, then the task thread would snapshot channel data, and finally receive file references to complete the checkpoint process. They also examined how to ensure consistent snapshots across multiple input channels and discussed the existing recovery logic that blocks upstream tasks until downstream recovery is complete. Rui agreed to create a document outlining the general approach before reorganizing the PR to reflect these changes.
Next steps
Roman
- Review the new document and updated PR from Rui and provide feedback on the proposed approach and implementation details.
Rui
- Organize and document the general approach for checkpointing and snapshotting, including the proposed three-step process and alternatives, and share with Roman for confirmation.
- Double-check the code to ensure that unblocking of upstream tasks during recovery works correctly for multiple input gates and channels, and confirm whether it is handled at the channel or input gate level.
- Reorganize the existing PR to reflect the new direction discussed (introducing new interfaces/classes for partial snapshot, control messages, and interaction between task thread and unspilling thread).
- Investigate and decide whether to reuse the task state snapshot format or introduce a new, simpler format for partial snapshot, and document the rationale.
- Review and confirm the interaction and synchronization between task thread and unspilling thread during snapshot, especially regarding reference passing and locking.
- Summarize the conclusions and open questions in a document and share with Roman for further review and discussion.
Summary
Control Message Checkpointing Implementation
Rui and Roman discussed implementing a control message approach for checkpointing to avoid extensive changes to input channels. They explored using a new interface for partial snapshots and discussed the challenges of maintaining data order during checkpointing. Roman raised concerns about synchronization between the spinning thread and task thread, particularly regarding when to snapshot channels versus files. They agreed that a control message system would be necessary to coordinate checkpoint operations across different input channels. Rui committed to further investigating the implementation details, particularly around multiple input gates and ensuring proper synchronization mechanisms are in place.
Disk Buffer Snapshot Implementation Discussion
Roman and Rui discussed the implementation of a snapshot mechanism for disk buffers and control messages in their system. Rui explained that the first control method would involve taking a snapshot of disk references and sending them to multiple input channels without reading actual data from disk. They debated whether task threads should directly checkpoint data to S3 or use a channel state writer, with Roman suggesting non-blocking methods to avoid blocking the task thread. The conversation ended with some confusion about the implementation details and the need for further clarification.
Checkpoint Mechanism Implementation Discussion
Roman and Rui discussed the implementation of a checkpoint mechanism involving task threads and unskilling threads. They clarified that the task thread would receive file information and offsets from the unskilling thread, with barriers coming from the network thread. They addressed concerns about maintaining consistent snapshots between input channels and discussed how barriers would be sent locally without requiring network buffers. The conversation ended with Rui seeking clarification on how checkpoints work, specifically whether the task thread receives the first checkpoint barrier.
System Barrier Implementation Discussion
Roman and Rui discussed the implementation of barriers in their system. They clarified that task threads only execute the first step initially, and when a barrier is received, they execute the second and third steps. Roman explained that barriers are used to ensure data consistency by discarding any data added after the barrier, and they don't need to wait for the barrier to take snapshots. Rui understood that the system uses logging and barriers to guarantee data correctness, and snapshots are taken at the channel level without waiting for all channels to complete.
Input Gates Implementation Discussion
Rui and Roman discussed implementing a solution for handling input gates and channel recovery. They agreed on using a unioned input gate approach and confirmed that the current implementation works as expected by unblocking upstream when all input and output channels are consumed. Rui planned to draft a document summarizing their conclusions and reorganize the PR, potentially removing unnecessary classes like the restored buffer store dispatcher. They also discussed the need for a new interface to allow interaction between task threads and submission threads for creating pre-filtered files.
Data Filtering System Implementation Discussion
Rui and Roman discussed implementing a data filtering system where filtered data would be written directly to disk without using a buffer in certain cases. They agreed that two buffers would be needed - one pre-filtered and one post-filtered buffer - to handle the deserialization, filtering, and writing process. Roman suggested reusing the same buffer in a loop, but Rui pointed out this could make the code more complex due to potential partial buffer consumption. They concluded that while the concept was clear, the specific implementation details, particularly regarding the dispatcher and task thread interface, still needed further consideration.
Task State Snapshot Format Discussion
Rui and Roman discussed implementing a checkpoint mechanism for task state snapshots, considering whether to reuse the existing format or introduce a new one. They identified that the current task state snapshot format might not be well-suited for their specific use case of partial recovery, as it was designed for full snapshots rather than their partial snapshot needs. Rui agreed to organize a new document outlining two approaches: reusing the existing task state snapshot format or introducing a new format if necessary, and will confirm with Roman once the document is ready.




on checkpoint, task thread:

1. Notify on-disk file component （unspilling thread）
  a. lock Unspiller
  b. receive reference to files (/tmp/xxx : 5036) by calling Unspiller.getCurrentPos()
2. Snapshot all channels
  for channel in channels do 
  channelStateWriter.addInputData… - until barrier from 1.a
3. channelStateWriter.addInputData (1.b)

note: introducing the lock and new control message is to prevent data duplication.

step1 确保了我们制作了一个一致性的快照。第二步的时候,快照Input Channel里面那些Buffer的时候,只会快照Barrier之前 buffers。 但是有可能某些channel还没有收到barrier,我们也不用担心,直接快照就够了。 因为我们的理解就是,一大第一步做完了,那么意味着一步现成没有一些正在进行的buffer,还没有添加到一部的channel里面。

也就是说,我们只要能进行快照了,那么就直接快照那些darker buffer就可以。 所以引入这个barrier只是说,barrier之后的数据要丢弃。 但是我们并不是说必须等到barrier才能快照,因为barrier马上就,如果即使没添加到一部的channel里面,应该也会立即添加,这个不用担心。 

```

class Unspiller {
  volatile String currentFileName;
  volatile long currentOffset;

  Synchronized <String, Long> getCurrentPos() {
      Return this…..
  }

  void unsell() {
  while (…) {
    synchronized (this) {
        currentOffset += ...
    }
  }
}
```