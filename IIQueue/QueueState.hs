module IIQueue.QueueState where

import Network
import IIQueue.Configuration
import Data.ByteString as BS

{-

Things we want to know about a QueueState:

oldestMessageAvailable,	memoryHasAMessage

Basically we need to know about the oldest
messages both on disk and in memory.

For the disk the oldest message is where the
pointer is right now. The should be something 
that communicates whether the handle is
available.

For the memory, the oldest message could be
the head of a list.

Things we want to do with a QueueState:

readMessageFromDisk, readMessageFromMemory

-}

data QueueState = QueueState {
	qsBufferUsed :: Int,
	qsBufferSize :: Int,
	qsPersistanceUsed :: Int,
	qsPersistanceSize :: Int,
	qsMaxMessageSize :: Int,
	qsMessages :: [Message]
}

mega :: Int -> Int
mega = (*) (1024 * 1024)

giga :: Int -> Int
giga = (*) (mega 1024)

newQueueState :: Configuration -> QueueState
newQueueState _ = QueueState 0 (mega 150) 0 (giga 100) (mega 100) []

data Message = Message Int Socket

data Transaction = Transaction {
	transactionBuffered :: Bool,
	transactionPersisted :: Bool,
	transactionProcessed :: Bool,
	transactionData :: BS.ByteString,
	transactionPersistanceId :: Int
}

data StorageType = Disk | DiskMemory

{-

The memory buffer worker:

It makes sure that the memory buffer is always being optimally used. This means that the oldest data is always in the memory buffer.

When memory is released the worker reads the oldest data on disk into memory.

Not sure if this makes sense, would it ever happen that when the memory buffer is made empty there is no consumer that would want to read from disk directly?

-}

{-

Receiving data worker: 

There are three cases, tried in order:

A reader is available: The message is directly streamed to reader, no memory is allocated, no data is saved. If the reader disconnects prematurely, the transaction fails and has to be retried.

Memory is free: Data is streamed to memory, and then to the hard disk.

Disk space is free: Data is streamed directly to disk.

-}
persist :: QueueState -> Message -> (QueueState, IO())
persist qs m 
	| memoryFreeDiskHasQueue qs m = (queueStateStore qs Disk m, saveToDisk m)
	| memoryFull qs m = (queueStateStore qs Disk m, saveToDisk m)
	| memoryFreeDiskEmpty qs m = (queueStateStore qs DiskMemory m, saveToBoth)
	where
		saveToBoth = do 
			strictMessage <- saveToMemory m
			saveToDisk strictMessage

		saveToDisk (Message len sock) = undefined len sock
		saveToDisk bs = undefined bs
		saveToMemory = undefined

{-
	queueStateStore is a helper function that updates the queueState with the size of the message that is to be saved.
-}
queueStateStore :: QueueState -> StorageType -> Message -> QueueState
queueStateStore qs Disk (Message len _) = qs
	{
		qsPersistanceUsed = (qsPersistanceUsed qs) - len
	} 
queueStateStore qs DiskMemory (Message len _) = qs
	{
		qsPersistanceUsed = (qsPersistanceUsed qs) - len,
		qsBufferUsed = (qsBufferUsed qs) - len
	} 

memoryFreeDiskHasQueue :: QueueState -> Message -> Bool
memoryFreeDiskHasQueue qs (Message len _) =
	qsBufferSize qs - qsBufferUsed qs >= len &&
	qsPersistanceUsed qs > 0

memoryFull :: QueueState -> Message -> Bool
memoryFull qs (Message len _) = 
	qsBufferSize qs - qsBufferUsed qs < len

memoryFreeDiskEmpty :: QueueState -> Message -> Bool
memoryFreeDiskEmpty qs (Message len _) =
	qsBufferSize qs - qsBufferUsed qs >= len &&
	qsPersistanceUsed qs == 0