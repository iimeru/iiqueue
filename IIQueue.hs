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

data Configuration = Configuration {}

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
startConnector c wsC rsC = connectorLoop $ newConnectorS c
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

{-

So the goal is to get messages from readers to writers.

When there is a reader:
	The reader will read the oldest message, whether it is on disk or in memory.

	When the oldest message is on disk but the disk is not available
	the oldest message in memory is read instead.

	If the memory is not available either, the reader is connected
	to a writer instead.

Whenever there is a writer with a message, but no reader
ready for it messages are buffered to memory and to disk.

Note: This system is fair, but most performant when there are
more than 2 readers, as the first reader is reading slowly
from disk, and the second is reading from memory. Any additional
readers will read directly from writers.
Note: The previous note only happens when at some point readers are
slower than writers.

-}
connectResources :: ConnectorS -> IO(ConnectorS)
connectResources cs@(ConnectorS ws rs _)
	-- TODO would this be faster if we iterated over all readers
	-- at once? Using the monadic fold thingy.
	| not $ null rs = connectReader cs 
	| not $ null ws = bufferWriter cs
	| otherwise = return cs

{- Connects a reader to a message. -}
connectReader :: ConnectorS -> IO(ConnectorS)
connectReader cs@(ConnectorS ws _ qs)
	| oldestMessageAvailable = readOldestMessage cs
	| memoryHasAMessage = readOldestMemoryMessage cs
	| writerAvailable = connectToWriter cs
	| otherwise = return cs
	where
		oldestMessageAvailable = undefined qs
		memoryHasAMessage = undefined qs
		writerAvailable = not $ null ws

{- Persists a message from a writer. -}
bufferWriter :: ConnectorS -> IO(ConnectorS)
bufferWriter cs = undefined cs

{-
 Starts listening for writers, puts something on the TChan when it's ready
 for message production.
-}
startWritersListener :: Configuration -> IO(TChan Socket)
startWritersListener c = undefined c

{-
 Starts listening for readers, puts the reader on the TChan when it's ready
 for message consumption.
-}
startReadersListener :: Configuration -> IO(TChan Socket)
startReadersListener = undefined

newQueueState :: Configuration -> QueueState
newQueueState c = undefined c

newConnectorS :: Configuration -> ConnectorS
newConnectorS c = ConnectorS [] [] $ newQueueState c

{-
 Reads the oldest message, from disk or from memory. It is passed to the
 first reader in the ConnectorS. 
-}
readOldestMessage :: ConnectorS -> IO(ConnectorS)
readOldestMessage = undefined

{-
 Reads the oldest message in memory. It is passed to the
 first reader in the ConnectorS. 
-}
readOldestMemoryMessage :: ConnectorS -> IO(ConnectorS)
readOldestMemoryMessage = undefined

{-
 Connects the first reader in ConnectorS to the first writer. 
-}
connectToWriter :: ConnectorS -> IO(ConnectorS)
connectToWriter = undefined

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