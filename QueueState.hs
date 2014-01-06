module QueueState where

import Network
import Configuration
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

Do we need to know whether the oldest message
on disk is older than the oldest message in 
memory?

Things we want to do with a QueueState:

readMessageFromDisk, readMessageFromMemory

-}

data QueueState = QueueState {
	qsBufferUsed :: Int,
	qsBufferSize :: Int,
	qsPersistanceUsed :: Int,
	qsPersistanceSize :: Int,
	qsMaxMessageSize :: Int
}

mega :: Int -> Int
mega = (*) (1024 * 1024)

giga :: Int -> Int
giga = (*) (mega 1024)

newQueueState :: Configuration -> QueueState
newQueueState _ = QueueState 0 (mega 150) 0 (giga 100) (mega 100)

data Message = Message Int Socket

data Transaction = Transaction {
	transactionBuffered :: Bool,
	transactionPersisted :: Bool,
	transactionProcessed :: Bool,
	transactionData :: BS.ByteString,
	transactionPersistanceId :: Int
}

data StorageType = Disk | DiskMemory