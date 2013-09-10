module IIQueue where

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC


data Transaction = Transaction {
  transactionBuffered :: Bool,
  transactionPersisted :: Bool,
  transactionProcessed :: Bool,
  transactionData :: BS.ByteString
}

data QueueState = QueueState {
  qsBufferUsed :: MVar Int,
  qsPersistanceUsed :: MVar Int,
  qsBufferSize :: Int,
  qsPersistanceSize :: Int,
  qsMaxMessageSize :: Int
}

putWorker :: QueueState -> Socket -> IO ()
putWorker qs socket = do
    headerBytes <- recv socket 8
    let (_, length) = parseHeader headerBytes

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

persist = undefined
buffer = undefined

main :: IO ()
main = do
  usedBufferVar <- newMVar 0
  usedPersistanceVar <- newMVar 0
  let queueState = QueueState usedBufferVar 128 usedPersistanceVar 1024 25 
  listenSock startListenSock
  forever $ do
    (sock; ) accept listenSock
    forkIO $ putWorker queueState sock

