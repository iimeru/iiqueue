module IIQueue where

import IIQueue.QueueState
import IIQueue.Configuration
import IIQueue.MessageBuffer
import Control.Concurrent.STM
import Network.Socket hiding (send, sendTo, recv, recvFrom)

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

The messageBuffer would be a pipe, messages get passed into it
and whenever needed they can be read back to readers.
-}

main :: IO ()
main = do
	configuration <- readConfiguration

	writers <- startWritersListener configuration
	readers <- startReadersListener configuration
	messageBuf <- startMessageBuffer configuration

	let channels = Channels writers readers messageBuf
	startConnector configuration channels

type WriterListener = TChan(Writer)
type ReaderListener = TChan(Reader)

type Writer = Socket
type Reader = Socket

data ConnectorS = ConnectorS {
	cnWriters :: [Writer],
	cnReaders :: [Reader]
}

data Channels = Channels {
	writersChannel :: WriterListener,
	readersChannel :: ReaderListener,
	messageBuffer :: MessageBuffer
}

startConnector :: Configuration -> Channels -> IO()
startConnector _ (Channels wsC rsC _) = connectorLoop $ ConnectorS [] []
	where
		connectorLoop cs = do
			ws' <- atomically $ appendChanToList wsC ws
			rs' <- atomically $ appendChanToList rsC rs
			cs' <- connectResources  $ ConnectorS ws' rs'

			connectorLoop cs'
			where
				ws = cnWriters cs
				rs = cnReaders cs

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
connectResources :: QueueState -> ConnectorS -> IO(QueueState,ConnectorS)
connectResources qs cs@(ConnectorS ws rs)
	-- TODO would this be faster if we iterated over all readers
	-- at once? Using the monadic fold thingy.
	| not $ null rs = connectReader qs cs 
	| not $ null ws = bufferWriter qs cs
	| otherwise = return (qs,cs)

{- Connects a reader to a message. -}
connectReader :: QueueState -> ConnectorS -> IO(QueueState,ConnectorS)
connectReader qs cs@(ConnectorS ws _)
	| oldestMessageAvailable = readOldestMessage qs cs
	| memoryHasAMessage = readOldestMemoryMessage qs cs
	| writerAvailable = connectToWriter qs cs
	| otherwise = return (qs,cs)
	where
		oldestMessageAvailable = undefined
		memoryHasAMessage = undefined
		writerAvailable = not $ null ws

{- Persists a message from a writer. -}
bufferWriter :: QueueState -> ConnectorS -> IO(QueueState,ConnectorS)
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

{-
startMessageBuffer starts a buffered pipe that accepts messages and from which messages can be read, the pipe is file backed.
-}
startMessageBuffer :: Configuration -> a
startMessageBuffer = undefined


newConnectorS :: Configuration -> ConnectorS
newConnectorS _ = ConnectorS [] []

{-
 Reads the oldest message, from disk or from memory. It is passed to the
 first reader in the ConnectorS. 
-}
readOldestMessage :: QueueState -> ConnectorS -> IO(ConnectorS,QueueState)
readOldestMessage = undefined

{-
 Reads the oldest message in memory. It is passed to the
 first reader in the ConnectorS. 
-}
readOldestMemoryMessage :: QueueState -> ConnectorS -> IO(ConnectorS,QueueState)
readOldestMemoryMessage = undefined

{-
 Connects the first reader in ConnectorS to the first writer. 
-}
connectToWriter :: QueueState -> ConnectorS -> IO(ConnectorS,QueueState)
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