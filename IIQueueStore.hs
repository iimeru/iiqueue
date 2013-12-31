module IIQueueStore where

import Prelude hiding (lookup)
import Data.Map
import Data.Maybe
import Data.Binary
import Data.ByteString
import System.Random
import Numeric
import Pipes
import Pipes.Prelude as P
import Pipes.Network.TCP (fromSocket)
import Network
import qualified System.IO as IO

-- FileName -> StoreFile
type StoreContext = Map String StoreFile
-- File Handle, Size, Offset
type StoreFile = (IO.Handle, Word64, Word64)

fileSize = 100000 -- 100MB

{- Naievely selects a file large enough to store the message in -}
selectFile :: StoreContext -> Word64 -> Maybe String
selectFile ctx len = listToMaybe goodFits
	where
		goodFits = [name | (name, (h,s,o)) <- elems ctx , s - o > len]

{- Adds the filesize to the Offset. -}
addMessageToContext :: StoreContext -> String -> Word64 -> StoreContext
addMessageToContext ctx name len = insert name file' ctx
	where
		(handle, size, offset) = lookup name ctx
		file' = (handle, size, offset + len)

{- Dumps a bytestream in a file handle. -}
writeToFile :: IO.Handle -> Word64 -> Producer ByteString IO () -> IO()
writeToFile file len bytes =
	hPut file $ encode len
	runEffect $ for (P.take len bytes) (liftIO . hPut file)

{- Returns an open file, either an existing one from the context, or
   a newly created one. -}
getFile :: StoreContext -> Int -> IO((StoreContext,String,IO.Handle))
getFile ctx len = maybe makeNewFile useExistingFile selectFile
	where
		newFilename = (showHex $ getStdRandom random :: Int) ++ ".iiq"
		newFile name = IO.openBinaryFile name IO.WriteMode
		makeNewFile = do
			name <- newFilename
			file <- newFile name
			let sf = (file, fileSize, 0)
			let ctx' = insert name sf ctx
			(ctx', name, file)
		useExistingFile n = return (ctx, n, lookup n ctx)

{- Persists a ByteString memory buffer to a file. -}
persistFromMemory :: StoreContext -> Int -> ByteString -> IO(StoreContext)
persistFromMemory ctx len bytestring = persist ctx len $ yield bytestring

{- Persists the data received from a socket to a file. -}
persistFromSocket :: StoreContext -> Int -> Socket -> IO(StoreContext)
persistFromSocket ctx len sock = persist ctx len $ fromSocket sock

{- Persist data from a producer to a file. -}
persist :: StoreContext -> Int -> Producer ByteString IO () -> IO(StoreContext)
persist ctx len bytes = do
	(ctx', name, file) <- getFile ctx totalLength
	writeToFile file len bytes
	addMessageToContext ctx' name totalLength
	where
		overhead = 8
		totalLength = overhead + len