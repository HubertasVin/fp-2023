{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Control.Monad.Free (Free (..), liftF)
import Data.List (find, isPrefixOf, intercalate, dropWhileEnd)
import Data.Time (UTCTime)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import GHC.RTS.Flags (ParFlags (parGcThreads))
import Lib2
import Data.Char (toLower)

type TableName = String

type FileContent = String

type Database = [Table]

type Table = (TableName, DataFrame)

type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile FilePath (FileContent -> next)
  | LoadFiles [TableName] ([FileContent] -> next)
  | GetTime (UTCTime -> next)
  | GetTableDfByName TableName [(TableName, DataFrame)] (DataFrame -> next)
  -- feel free to add more constructors here
  deriving (Functor)

data YamlData = YamlData { tables :: [YamlTable] } deriving (Show)
data YamlTable = YamlTable { name :: String } deriving (Show)

type Execution = Free ExecutionAlgebra

loadFile :: FilePath -> Execution FileContent
loadFile path = liftF $ LoadFile path id

loadFiles :: [TableName] -> Execution [FileContent]
loadFiles names = liftF $ LoadFiles names id

getTableDfByName :: TableName -> [(TableName, DataFrame)] -> Execution DataFrame
getTableDfByName tableName tables = liftF $ GetTableDfByName tableName tables id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
  Left err -> return $ Left err
  Right statement -> do
    case statement of
      (Now columnNames tableName maybeOperator) -> do
        currentTime <- getTime
        return $ Right $ DataFrame [Column "CurrentTime" StringType] [[StringValue (show currentTime)]]
      _ -> do
        let result = executeStatement statement
        return $ case result of
          Left err -> Left err
          Right df -> Right df

-- TODO Convert Yaml string to Table
-- Load file with modified table names
loadFileWithModifiedNames :: FilePath -> Execution String
loadFileWithModifiedNames file = do
  fileContent <- loadFile ("db/" ++ file ++ ".yaml")
  return $ unlines $ map ("db/" ++) $ map (++ ".yaml") $ splitOn ", " $ drop 10 fileContent

-- Load files based on modified table names
loadFilesWithModifiedNames :: [FilePath] -> Execution [String]
loadFilesWithModifiedNames tableNames = mapM loadFileWithModifiedNames tableNames

splitOn :: Eq a => [a] -> [a] -> [[a]]
splitOn _ [] = []
splitOn delimiter lst =
  let (before, remainder) = breakList delimiter lst
  in before : case remainder of
               [] -> []
               x -> if x == delimiter
                      then []
                      else splitOn delimiter (drop (length delimiter) x)

breakList :: Eq a => [a] -> [a] -> ([a], [a])
breakList _ [] = ([], [])
breakList delim s@(x:xs)
  | delim `isPrefixOf` s = ([], drop (length delim) s)
  | otherwise = let (before, after) = breakList delim xs
                in (x:before, after)

parseYaml :: String -> YamlData
parseYaml yamlText = YamlData { tables = parseTables (lines yamlText) }

parseTables :: [String] -> [YamlTable]
parseTables [] = []
parseTables (line:rest) =
  case words line of
    ["-", "name:", tableName] -> YamlTable tableName : parseTables rest
    _ -> parseTables rest

-- Function to extract table names from YAML data
extractTableNames :: YamlData -> [String]
extractTableNames (YamlData tables) = map name tables





-- TODO Convert Table to YAML string
-- Function to convert ColumnType to YAML string
columnTypeToYaml :: ColumnType -> String
columnTypeToYaml IntegerType = "IntegerType"
columnTypeToYaml StringType = "StringType"
columnTypeToYaml BoolType = "BoolType"

-- Function to convert Value to YAML string
valueToYaml :: Value -> String
valueToYaml (IntegerValue int) = show int
valueToYaml (StringValue str) = "\"" ++ str ++ "\""
valueToYaml (BoolValue bool) = if bool then "true" else "false"
valueToYaml NullValue = "null"

-- Function to convert Column to YAML string
columnToYaml :: Column -> String
columnToYaml (Column name columnType) =
  "    - name: "
    ++ name
    ++ "\n"
    ++ "      type: "
    ++ columnTypeToYaml columnType

-- Function to convert Row to YAML string
rowToYaml :: Row -> String
rowToYaml row = "    - [" ++ intercalate ", " (map valueToYaml row) ++ "]"

-- Function to convert DataFrame to YAML string
tableToYaml :: Table -> String
tableToYaml (tableName, DataFrame columns rows) =
  unlines $ map tableToYaml [(tableName, columns, rows)]
  where
    tableToYaml :: (String, [Column], [Row]) -> String
    tableToYaml (tableName1, columns1, rows1) =
      "- table: "
        ++ tableName1
        ++ "\n"
        ++ "  columns:\n"
        ++ unlines (map columnToYaml columns1)
        ++ "  rows:\n"
        ++ unlines (map rowToYaml rows1)
