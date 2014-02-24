module FileStore where

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
import qualified System.IO as IO
import Control.Concurrent (MVar, newMVar, takeMVar, putMVar, forkIO)

-- FileName -> StoreFile
type StoreContext = Map String StoreFile
-- File Handle, Size, Offset
type StoreFile = (IO.Handle, Word64, Word64)
-- Size, Producer
type Message = (Word64, Producer ByteString IO ())

fileSize :: Word64
fileSize = 100000000-- 100MB

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
	let header = Data.ByteString.concat $ BL.toChunks $ encode len
	hPut file header
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

{- Persist data from a producer to a file. -}
persist :: StoreContext -> Message -> IO(StoreContext)
persist ctx (len, bytes) = do
	-- Gets a file, makes it if one doesn't exist
	(ctx', name, file) <- getFile ctx totalLength
	-- Writes the bytes to the file
	writeToFile file len bytes
	-- Flushes the file
	IO.hFlush file
	-- Updates the context
	return $ addMessageToContext ctx' name totalLength
	where
		overhead = 8
		totalLength = overhead + len

makeContext :: StoreContext
makeContext = Data.Map.empty

data AsyncFileStoreContext = AsyncFileStoreContext {
	storeContext :: MVar StoreContext,
	storeWorking :: MVar Bool
}

{-
  The goal of the async file store is to be able to dispatch the writing
  of files to it, and it will answer if it is busy writing at the moment.
  I don't think it is necessary for it to queue workers at the moment, it
  will just refuse to work when it is busy.
-}
asyncFileStore :: IO(AsyncFileStoreContext) 
asyncFileStore = do
	working <- newMVar False
	ctx <- newMVar makeContext
	return $ AsyncFileStoreContext ctx working

-- If this function returns true it assumes you are going to work
-- on it, so it flips the value. 
asyncFileStoreAvailableAndFlip :: AsyncFileStoreContext -> IO(Bool)
asyncFileStoreAvailableAndFlip ctx = do
	working <- takeMVar $ storeWorking ctx
	if not working
		then do
			putMVar (storeWorking ctx) True
		else do
			putMVar (storeWorking ctx) False
	return $ not working

-- This function assumes the filestore is available.
asyncFileStorePersist :: AsyncFileStoreContext -> Message -> IO()
asyncFileStorePersist actx m = do
	forkIO $ do
		ctx <- takeMVar $ storeContext actx
		ctx' <- persist ctx m
		putMVar (storeContext actx) ctx'
		takeMVar $ storeWorking actx
		putMVar (storeWorking actx) False
	return ()

{- The reader of the file store must write to disk the filename, length and
offset of the message it has read. This is so that the state can always be
reconstructed.-}

main :: IO()
main = do 
	persist ctx (fromIntegral l, yield bytes)
	return ()
	where
		ctx = makeContext
		l = Data.ByteString.length bytes
		bytes = Data.ByteString.concat $ BL.toChunks $ encode "Hello World"
