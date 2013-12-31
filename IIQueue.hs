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
persist qs m | memoryFreeDiskHasQueue qs m = (queueStateStore qs Disk m, saveToDisk m)
             | memoryFull qs m = (queueStateStore qs Disk m, saveToDisk m)
             | memoryFreeDiskEmpty qs m = (queueStateStore qs DiskMemory m, saveToBoth)
  where
    saveToBoth = do 
      strictMessage <- saveToMemory m
      saveToDisk strictMessage

--
-- Iets wordt geappend aan de queue. Yield een nieuwe queue met het laatste element bovenop. Een
-- linked list is waarschijnlijk het handigst. Dus een QueueState heeft een link naar het nieuwste
-- element en het oudste element.
--
-- Er is ook een link naar het oudste element op de schijf, omdat die wellicht ingeswapped moet
-- worden.
-- 
-- Performance ding: als er geheugen vrij is moet dat gebruikt worden, dus het moet mogelijk zijn
-- voor nieuwe elementen om voor te pikken in de memory queue. Volgorde is niet zo heel belangrijk.
-- Maar om eerlijkheid van de queue te garanderen mag het niet zo zijn dat volgorde wordt genegeerd.
--
-- Hoe staan dingen op de hardeschijf?
--
-- We willen messages van enkele bytes tot honderden mb's ondersteunen. Maar vooral veel kleine messages
-- dus we gebruiken geprealloceerde files die meerdere messages bevatten. Omdat het een queue is, kunnen
-- we er een cyclische buffer van maken.
--
-- Dus iedere file heeft een lengte, en een 0 index.
-- Iedere message heeft een offset en een length. 
--
-- Er mag maar 1 concurrente lezer zijn, en maar 1 concurrente schrijver. Dit om contentie op de hdd te
-- voorkomen.  
-- 
-- Waarom zouden er meerdere files zijn? Om te voorkomen dat alle benodigde ruimte geprealloceerd moet zijn.
-- Willen we dat echt? ... ik weet het niet..
-- 

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
