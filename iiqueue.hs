module IIQueue where

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
  transactionData :: BS.ByteString
}

data QueueState = QueueState {
  qsBufferUsed :: Int,
  qsBufferSize :: Int,
  qsPersistanceUsed :: Int,
  qsPersistanceSize :: Int,
  qsMaxMessageSize :: Int
}

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

-- Er zijn 2 gevallen:
--
-- geheugen vrij: data gaat eerst in het geheugen, dan op de schijf
-- niet genoeg geheugen vrij: data gaat op de schijf
--
-- data gaat naar de schijf met een lazy bytestring
-- data staat in het geheugen in een gewone bytestring
--
-- de memory buffer worker zorgt ervoor dat de memory buffer optimaal wordt gebruikt. Dat
-- betekent dat de oudste data altijd in de memory buffer staat.
-- dat betekent dat initieel als er data binnenkomt dat deze meteen naar de memory buffer
-- gaat. wanneer deze vol is stopt het met lezen van de input.
--
-- Wanneer er ruimte vrij komt in de buffer controleert de memory buffer of er ongebufferde data
-- op de hardeschijf staat. Als dat zo is dan leest het die naar de buffer. Als het niet zo is
-- dan start hij weer met lezen van de netwerk input.
--
-- De hardeschijf putWorker leest van de input en schrijft naar de hardeschijf. Om concurrent
-- van de schijf te lezen (om meerdere concurrente transacties te ondersteunen) kunnen we meerdere
-- voorgealloceerde bestanden gebruiken. Om fragmentatie te voorkomen geen bestand per transactie.  

persist :: QueueState -> Message -> (QueueState, IO())
persist qs m | memoryFreeDiskHasQueue qs m = (queueStateStore qs Disk message, saveToDisk message)
             | memoryFull qs m = (queueStateStore qs Disk message, saveToDisk message)
             | memoryFreeDiskEmpty qs m = (queueStateStore qs DiskMemory message, saveToBoth)
  where
    saveToBoth = do 
      strictMessage <- saveToMemory message
      saveToDisk strictMessage

saveToDisk = undefined
saveToMemory = undefined

data StorageType = Disk | DiskMemory
queueStateStore :: QueueState -> StorageType -> Message -> QueueState
queueStateStore qs Disk m = qs { qsPersistanceUsed = (qsPersistanceUsed qs) - (messageSize m)} 
queueStateStore qs DiskMemory m = qs { qsPersistanceUsed = (qsPersistanceUsed qs) - (messageSize m),
                                       qsBufferUsed = (qsBufferUsed qs) - (messageSize m)
                                     } 


-- TODO rewrite in terms of eachother
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
  let queueState = QueueState usedBufferVar 128 usedPersistanceVar 1024 25 
  queueStateVar <- newMVar queueState
  listenSock startListenSock
  forever $ do
    (sock,_) <- accept listenSock
    forkIO $ putWorker queueStateVar sock

