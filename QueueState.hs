module QueueState where

import Network
import Configuration
import Data.ByteString as BS

{-
Things we want to know about a QueueState:

oldestMessageAvailable,	memoryHasAMessage

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

newQueueState :: Configuration -> QueueState
newQueueState c = undefined c

data Message = Message Int Socket

data Transaction = Transaction {
	transactionBuffered :: Bool,
	transactionPersisted :: Bool,
	transactionProcessed :: Bool,
	transactionData :: BS.ByteString,
	transactionPersistanceId :: Int
}

data StorageType = Disk | DiskMemory
