module IIQueueStore where

import Prelude hiding (lookup)
import Data.Map
import Data.Maybe
import Data.Binary
import Data.ByteString
import qualified Data.ByteString.Lazy as BL
import System.Random
import Numeric
import Pipes
import qualified Pipes.Prelude as P
import Pipes.Network.TCP (fromSocket)
import Network
import qualified System.IO as IO

-- FileName -> StoreFile
type StoreContext = Map String StoreFile
-- File Handle, Size, Offset
type StoreFile = (IO.Handle, Word64, Word64)

fileSize :: Word64
fileSize = 100000-- 100MB

{- Naievely selects a file large enough to store the message in -}
selectFile :: StoreContext -> Word64 -> Maybe String
selectFile ctx len = listToMaybe goodFits
	where
		goodFits = [name | (name, (_,s,o)) <- assocs ctx , s - o > len]

{- Adds the filesize to the Offset. -}
addMessageToContext :: StoreContext -> String -> Word64 -> StoreContext
addMessageToContext ctx name len = insert name file' ctx
	where
		(handle, s, offset) = fromJust $ lookup name ctx
		file' = (handle, s, offset + len)

{- Dumps a bytestream in a file handle. -}
writeToFile :: IO.Handle -> Word64 -> Producer ByteString IO () -> IO()
writeToFile file len bytes = do
	mapM (hPut file) $ BL.toChunks $ encode len
	runEffect $ for bytes (liftIO . hPut file) >-> P.take (fromIntegral len)

{- Returns an open file, either an existing one from the context, or
   a newly created one. -}
getFile :: StoreContext -> Word64 -> IO((StoreContext,String,IO.Handle))
getFile ctx len = maybe makeNewFile useExistingFile (selectFile ctx len)
	where
		newFilename = do
			randomName <- getStdRandom random :: IO(Word64)
		 	return $ showHex randomName ".iiq"
		newFile name = IO.openBinaryFile name IO.WriteMode
		makeNewFile = do
			name <- newFilename
			file <- newFile name
			let sf = (file, fileSize, 0)
			let ctx' = insert name sf ctx
			return (ctx', name, file)
		useExistingFile n = return (ctx, n, existingHandle)
			where
				(existingHandle,_,_) = fromJust $ lookup n ctx

{- Persists a ByteString memory buffer to a file. -}
persistFromMemory :: StoreContext -> Word64 -> ByteString -> IO(StoreContext)
persistFromMemory ctx len bytestring = persist ctx len $ yield bytestring

{- Persists the data received from a socket to a file. -}
persistFromSocket :: StoreContext -> Word64 -> Socket -> IO(StoreContext)
persistFromSocket ctx len sock = persist ctx len $ fromSocket sock 4096

{- Persist data from a producer to a file. -}
persist :: StoreContext -> Word64 -> Producer ByteString IO () -> IO(StoreContext)
persist ctx len bytes = do
	(ctx', name, file) <- getFile ctx totalLength
	writeToFile file len bytes
	let ctx'' = addMessageToContext ctx' name totalLength
	return ctx''
	where
		overhead = 8
		totalLength = overhead + len