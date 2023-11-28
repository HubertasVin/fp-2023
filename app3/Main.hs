module Main (main) where

import Control.Monad.Free (Free (..))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Functor ((<&>))
import Data.List qualified as L
import Data.Time (UTCTime, getCurrentTime)
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import System.Directory (doesFileExist, removeFile, renameFile)
import System.IO (IOMode (ReadMode), hClose, withFile)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names =
        [ "select",
          "*",
          "from",
          "employees",
          "employeesSalary",
          "flags",
          "specimen",
          "show",
          "table",
          "tables",
          "insert",
          "into",
          "update",
          "set",
          "delete",
          "values",
          "set",
          "update",
          "delete",
          "where"
        ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
  next <- runStep step
  runExecuteIO next
  where
    -- probably you will want to extend the interpreter
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
    runStep (Lib3.LoadFile next) = do
      let path = "db/tables.yaml"
      exists <- doesFileExist path
      exists2 <- doesFileExist $ "src/" ++ path
      let finalPath
            | exists = path
            | exists2 = "src/" ++ path
            | otherwise = error $ "File not found: " ++ path
      fileContents <- readFile finalPath
      withFile finalPath ReadMode $ \handle -> hClose handle
      return $ next fileContents
    runStep (Lib3.SaveFile fileContents next) = do
      let path = "db/tablesTemp.yaml"
      exists <- doesFileExist path
      exists2 <- doesFileExist $ "src/" ++ path
      let finalPath
            | exists = path
            | exists2 = "src/" ++ path
            | otherwise = error $ "File not found: " ++ path
      writeFile finalPath fileContents
      return next
    runStep (Lib3.DeleteFile next) = do
      let path = "db/tablesTemp.yaml"
      exists <- doesFileExist path
      exists2 <- doesFileExist $ "src/" ++ path
      let finalPath
            | exists = path
            | exists2 = "src/" ++ path
            | otherwise = error $ "File not found: " ++ path
      removeFile "db/tablesTemp.yaml"
      return next
    runStep (Lib3.RenameFile next) = do
      let pathSrc = "db/tablesTemp.yaml"
      let pathDst = "db/tables.yaml"
      existsSrc <- doesFileExist pathSrc
      existsSrc2 <- doesFileExist $ "src/" ++ pathSrc
      existsDst <- doesFileExist pathDst
      existsDst2 <- doesFileExist $ "src/" ++ pathDst
      let finalPathSrc
            | existsSrc = pathSrc
            | existsSrc2 = "src/" ++ pathSrc
            | otherwise = error $ "File not found: " ++ pathSrc
      let finalPathDst
            | existsDst = pathDst
            | existsDst2 = "src/" ++ pathDst
            | otherwise = error $ "File not found: " ++ pathDst
      renameFile finalPathSrc finalPathDst
      return next