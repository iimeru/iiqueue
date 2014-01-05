module IIQueue where

import Control.Concurrent.STM
import Network.Socket hiding (send, sendTo, recv, recvFrom)

import qualified Data.ByteString as BS

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
	qsMaxMessageSize :: Int
}

data StorageType = Disk | DiskMemory


{-

A writerlistener needs:

- To sink a message when possible.

A readerListener needs:

- To be passed messages when they are available.

So writers and readers basically notify their availability.

A single thread connects readers to writers.

- To fetch a reader if there is one available
- To fetch memory space if it is available
- To fetch diskspace if it is available

-}

data Configuration = Configuration {}

main :: IO ()
main = do
	configuration <- readConfiguration

	writers <- startWritersListener configuration
	readers <- startReadersListener configuration

	startConnector configuration writers readers


readConfiguration :: IO(Configuration)
readConfiguration = undefined

data ConnectorS = ConnectorS {
	cnWriters :: [Socket],
	cnReaders :: [Socket],
	cnQueueState :: QueueState
}

startConnector :: Configuration -> TChan(Socket) -> TChan(Socket) -> IO()
startConnector c wsC rsC = connectorLoop $ newQueueState c
	where
		connectorLoop cs = do
			ws' <- atomically $ appendChanToList wsC ws
			rs' <- atomically $ appendChanToList rsC rs
			cs' <- connectResources $ ConnectorS ws' rs' qs

			connectorLoop cs'
			where
				ws = cnWriters cs
				rs = cnReaders cs
				qs = cnQueueState cs

		appendChanToList :: TChan(a) -> [a] -> STM([a])
		appendChanToList chan list = do
			empty <- isEmptyTChan chan
			if empty then
				return list
				else do
					newVal <- readTChan chan
					return $ newVal : list

connectResources :: ConnectorS -> IO(ConnectorS)
connectResources cs 
	| ws == [] && rs == [] = return cs
	| ws == [] = handleJustReaders cs
	| otherwise = do
		connectWriterToReader w r
		connectResources restCS
	where
		ws@[w:rws] = cnWriters cs
		rs@[r:rrs] = cnReaders cs
		qs = cnQueueState cs
		restCS = cs { cnWriters = rws, cnReaders = rrs}


startWritersListener = undefined
startReadersListener = undefined
newQueueState c = undefined

{-}
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
-}
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
{-
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
-}