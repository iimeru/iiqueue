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

data Item = Item {
  itemTransaction :: Transaction,
  itemNext :: Maybe MemoryItem
}

parseHeader = undefined

putWorker :: MVar(QueueState) -> Socket -> IO ()
putWorker mqs socket = do
    headerBytes <- recv socket 8
    let (_, length) = parseHeader headerBytes
    qs <- takeMVar mqs
    if (length <= (qsMaxMessageSize qs))
      then do
        let (qs',actions) = persist qs (Message length socket)
        putMVar mqs qs'
        actions
      else do
        putMVar mqs qs
    putWorker mqs socket

{-

Receiving data worker: 

There are three cases, tried in order:

A reader is available: The message is directly streamed to reader, no memory is allocated, no data is saved. If the reader disconnects prematurely, the transaction fails and has to be retried.

Memory is free: Data is streamed to memory, and then to the hard disk.

Disk space is free: Data is streamed directly to disk.

-}

{-

The memory buffer worker:

It makes sure that the memory buffer is always being optimally used. This means that the oldest data is always in the memory buffer.

When memory is released the worker reads the oldest data on disk into memory.

Not sure if this makes sense, would it ever happen that when the memory buffer is made empty there is no consumer that would want to read from disk directly?

-}

persist :: QueueState -> Message -> (QueueState, IO())
persist qs m | memoryFreeDiskHasQueue qs m = (queueStateStore qs Disk m, saveToDisk m)
             | memoryFull qs m = (queueStateStore qs Disk m, saveToDisk m)
             | memoryFreeDiskEmpty qs m = (queueStateStore qs DiskMemory m, saveToBoth)
  where
    saveToBoth = do 
      strictMessage <- saveToMemory m
      saveToDisk strictMessage

saveToDisk (Message length sock) = undefined
saveToDisk bs = 
saveToMemory = undefined

data StorageType = Disk | DiskMemory
queueStateStore :: QueueState -> StorageType -> Message -> QueueState
queueStateStore qs Disk (Message length _) = qs { qsPersistanceUsed = (qsPersistanceUsed qs) - length} 
queueStateStore qs DiskMemory (Message length _) = qs { qsPersistanceUsed = (qsPersistanceUsed qs) - length,
                                       qsBufferUsed = (qsBufferUsed qs) - length
                                     } 

memoryFreeDiskHasQueue qs (Message length _) =
    qsBufferSize qs - qsBufferUsed qs >= length &&
    qsPersistanceUsed qs > 0
memoryFull qs (Message length _) = 
    qsBufferSize qs - qsBufferUsed qs < length
memoryFreeDiskEmpty qs (Message length _) =
    qsBufferSize qs - qsBufferUsed qs >= length &&
    qsPersistanceUsed qs == 0

main :: IO ()
main = do
  let queueState = QueueState 0 128 0 1024 25 
  queueStateVar <- newMVar queueState
  listenSock <- socket AF_INET Stream 6 
  listen listenSock 100
  forever $ do
    (sock,_) <- accept listenSock
    forkIO $ putWorker queueStateVar sock
