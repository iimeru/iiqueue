module IIQueue where

import Control.Monad
import Control.Concurrent
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC

data Message = Message Int Socket

data Transaction = Transaction {
	transactionBuffered :: Bool,
	transactionPersisted :: Bool,
	transactionProcessed :: Bool,
	transactionData :: BS.ByteString,
	transactionPersistanceId :: Int
}

data QueueState = QueueState {
	qsBufferUsed :: Int,
	qsBufferSize :: Int,
	qsPersistanceUsed :: Int,
	qsPersistanceSize :: Int,
	qsMaxMessageSize :: Int,
	qsNewestItem :: Item,
	qsOldestItem :: Item,
	qsOldestDiskItem :: Item
}

data StorageType = Disk | DiskMemory

data Item = Item {
	itemTransaction :: Transaction,
	itemNext :: Maybe MemoryItem
}

data Context {}
{-

A writerlistener needs:

- To sink a message when possible.

A readerListener needs:

- To be passed messages when they are available.

So writers and readers basically notify their availability. 

- To fetch a reader if there is one available
- To fetch memory space if it is available
- To fetch diskspace if it is available

-}

data Configuration {}


main :: IO ()
main = do
	configuration <- readConfiguration
	context <- initializeContext configuration

	startWritersListener configuration context
	startReadersListener configuration context

readConfiguration :: IO(Configuration)
readConfiguration = undefined

initializeContext :: Configuration -> IO(Context)
initializeContext c = undefined

startWritersListener :: Configuration -> Context -> IO()
startWritersListener = forkIO $ do
	let queueState = QueueState 0 128 0 1024 25 
	queueStateVar <- newMVar queueState
	listenSock <- socket AF_INET Stream 6 
	listen listenSock 100
	forever $ do
		(sock,_) <- accept listenSock
		forkIO $ writerLoop queueStateVar sock

parseHeader = undefined

writerLoop :: MVar(QueueState) -> Socket -> IO ()
putWorker mqs socket = do
	headerBytes <- recv socket 8
	let (_, len) = parseHeader headerBytes
	qs <- takeMVar mqs
	if (len <= (qsMaxMessageSize qs)) then do
		let (qs',actions) = persist qs (Message len socket)
		putMVar mqs qs'
		actions
	else do
		putMVar mqs qs
		writerLoop mqs socket

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

	saveToDisk (Message len sock) = undefined
	saveToDisk bs = undefined
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
