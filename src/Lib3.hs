{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Data.Yaml as Yaml
import Text.ParserCombinators.Parsec
import Data.Text.Encoding (encodeUtf8)
import Data.Aeson (FromJSON, parseJSON)
import qualified Data.Vector as V
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as BS
import Control.Monad.Free (Free (..), liftF)
import Data.List (find, isPrefixOf, intercalate, dropWhileEnd)
import Data.Time (UTCTime)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import GHC.RTS.Flags (ParFlags (parGcThreads))
import Lib2
import Data.Char (toLower)
import Data.Maybe

type TableName = String

type FileContent = String

-- In Lib3.hs
newtype Database = Database { unDatabase :: [(TableName, DataFrame)] }

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

instance FromJSON Database where
  parseJSON = parseDatabase

loadFile :: FilePath -> Execution FileContent
loadFile path = liftF $ LoadFile path id

loadFiles :: [TableName] -> Execution [FileContent]
loadFiles names = liftF $ LoadFiles names id

getTableDfByName :: TableName -> [(TableName, DataFrame)] -> Execution DataFrame
getTableDfByName tableName tables = liftF $ GetTableDfByName tableName tables id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id





-- Execute SQL
-- executeSql :: String -> Execution (Either ErrorMessage DataFrame)
-- executeSql sql = do
--   currentTime <- getTime
--   tableNamesFileContent <- loadFile "db/tables.yaml"
--   let yamlData = parseYaml tableNamesFileContent
--   let tableNames = extractTableNames yamlData
--   tableContents <- loadFilesWithModifiedNames tableNames
--   return $ Left $ "Not implemented yet" ++ " " ++ concat tableContents
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
  Left err -> return $ Left err
  Right statement -> do
    employeeTableContents <- loadFile "db/tables.yaml"
    let generatedDatabase = yamlToDatabase employeeTableContents
    case statement of
      LoadTable -> do
        let result = executeStatement statement (unDatabase generatedDatabase)
        return $ case result of
          Left err -> Left $ fst (head (unDatabase generatedDatabase)) ++ "\n" ++ err
          Right df -> Right df
      _ -> do
        let result = executeStatement statement (unDatabase generatedDatabase)
        return $ case result of
          Left err -> Left err
          Right df -> Right df




-- TODO Convert Yaml string to Table
-- parseColumnType :: String -> ColumnType
-- parseColumnType "IntegerType" = IntegerType
-- parseColumnType "StringType"  = StringType
-- parseColumnType "BoolType"    = BoolType
-- parseColumnType _             = error "Invalid column type"

-- parseColumn :: Object -> Parser Column
-- parseColumn obj = do
--   name <- obj .: "name"
--   typeStr <- obj .: "type"
--   return $ Column name (parseColumnType typeStr)

-- parseValue :: Yaml.Value -> Parser DataFrame.Value
-- parseValue (Yaml.Number n) = return $ DataFrame.IntegerValue (floor n)
-- parseValue (Yaml.String s) = return $ DataFrame.StringValue (T.unpack s)
-- parseValue (Yaml.Bool b)   = return $ DataFrame.BoolValue b
-- parseValue Yaml.Null       = return $ DataFrame.NullValue
-- parseValue _               = fail "Invalid value"

-- parseRow :: [Yaml.Value] -> Parser [DataFrame.Value]
-- parseRow = mapM parseValue


databaseToString :: Database -> String
databaseToString (Database tables) = unlines $ map tableToString tables
  where
    tableToString :: Table -> String
    tableToString (tableName, DataFrame columns rows) =
      unlines $ map tableToString [(tableName, columns, rows)]
      where
        tableToString :: (String, [DataFrame.Column], [Row]) -> String
        tableToString (tableName1, columns1, rows1) =
          "- table: "
            ++ tableName1
            ++ "\n"
            ++ "  columns:\n"
            ++ unlines (map columnToString columns1)
            ++ "  rows:\n"
            ++ unlines (map rowToString rows1)

        columnTypeToString :: ColumnType -> String
        columnTypeToString IntegerType = "IntegerType"
        columnTypeToString StringType = "StringType"
        columnTypeToString BoolType = "BoolType"

        valueToString :: DataFrame.Value -> String
        valueToString (IntegerValue int) = show int
        valueToString (StringValue str) = "\"" ++ str ++ "\""
        valueToString (BoolValue bool) = if bool then "true" else "false"
        valueToString NullValue = "null"

        columnToString :: DataFrame.Column -> String
        columnToString (Column name columnType) =
          "    - name: "
            ++ name
            ++ "\n"
            ++ "      type: "
            ++ columnTypeToString columnType

        rowToString :: Row -> String
        rowToString row = "    - [" ++ intercalate ", " (map valueToString row) ++ "]"

yamlToDatabase :: FileContent -> Database
yamlToDatabase yamlContent =
  case decodeEither' (encodeUtf8 $ T.pack yamlContent) of
    Left err -> error $ "YAML parsing error: " ++ show err
    Right val -> val

parseDatabase :: Yaml.Value -> Yaml.Parser Database
parseDatabase (Yaml.Array tables) = do
  tables' <- traverse parseTable (V.toList tables)
  return $ Database tables'
parseDatabase _ = fail "Invalid database format"

parseTable :: Yaml.Value -> Yaml.Parser Table
parseTable (Yaml.Object obj) = do
  tableName <- obj .: "table" >>= parseString
  columns <- obj .: "columns" >>= parseColumns
  rows <- obj .: "rows" >>= parseRows
  return (tableName, DataFrame columns rows)
  where
    parseString :: Yaml.Value -> Yaml.Parser String
    parseString (Yaml.String s) = return $ T.unpack s
    parseString _ = fail "Invalid value, expected String"

    parseColumnType :: String -> ColumnType
    parseColumnType "IntegerType" = IntegerType
    parseColumnType "StringType"  = StringType
    parseColumnType "BoolType"    = BoolType
    parseColumnType _             = error "Invalid column type"

    parseColumn :: Yaml.Value -> Yaml.Parser DataFrame.Column
    parseColumn (Yaml.Object colObj) = do
      name <- colObj .: "name" >>= parseString
      typeStr <- colObj .: "type" >>= parseString
      return $ Column name (parseColumnType typeStr)
    parseColumn _ = fail "Invalid column format"

    parseColumns :: Yaml.Value -> Yaml.Parser [DataFrame.Column]
    parseColumns (Yaml.Array colArray) = mapM parseColumn (V.toList colArray)
    parseColumns _ = fail "Invalid columns format"

    parseValue :: Yaml.Value -> Yaml.Parser DataFrame.Value
    parseValue val = case parseValue' val of
      Left err -> fail err
      Right v -> return v
      where
        parseValue' :: Yaml.Value -> Either String DataFrame.Value
        parseValue' (Yaml.Number n) = Right $ DataFrame.IntegerValue (floor n)
        parseValue' (Yaml.String s) = Right $ DataFrame.StringValue (T.unpack s)
        parseValue' (Yaml.Bool b)   = Right $ DataFrame.BoolValue b
        parseValue' Yaml.Null       = Right DataFrame.NullValue
        parseValue' _               = Left "Invalid value"

    parseRow :: Yaml.Value -> Yaml.Parser [DataFrame.Value]
    parseRow (Yaml.Array rowArray) = mapM parseValue (V.toList rowArray)
    parseRow _ = fail "Invalid row format"

    parseRows :: Yaml.Value -> Yaml.Parser [Row]
    parseRows (Yaml.Array rowsArray) = mapM parseRow (V.toList rowsArray)
    parseRows _ = fail "Invalid rows format"

parseTable _ = fail "Invalid table format"






-- Load file with modified table names
{- loadFileWithModifiedNames :: FilePath -> Execution String
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
extractTableNames (YamlData tables) = map name tables -}





-- TODO Convert Table to YAML string
-- Function to convert DataFrame to YAML string
tableToYaml :: Table -> String
tableToYaml (tableName, DataFrame columns rows) =
  unlines $ map tableToYaml [(tableName, columns, rows)]
  where
    tableToYaml :: (String, [DataFrame.Column], [Row]) -> String
    tableToYaml (tableName1, columns1, rows1) =
      "- table: "
        ++ tableName1
        ++ "\n"
        ++ "  columns:\n"
        ++ unlines (map columnToYaml columns1)
        ++ "  rows:\n"
        ++ unlines (map rowToYaml rows1)

-- Function to convert ColumnType to YAML string
columnTypeToYaml :: ColumnType -> String
columnTypeToYaml IntegerType = "IntegerType"
columnTypeToYaml StringType = "StringType"
columnTypeToYaml BoolType = "BoolType"

-- Function to convert Value to YAML string
valueToYaml :: DataFrame.Value -> String
valueToYaml (IntegerValue int) = show int
valueToYaml (StringValue str) = "\"" ++ str ++ "\""
valueToYaml (BoolValue bool) = if bool then "true" else "false"
valueToYaml NullValue = "null"

-- Function to convert Column to YAML string
columnToYaml :: DataFrame.Column -> String
columnToYaml (Column name columnType) =
  "    - name: "
    ++ name
    ++ "\n"
    ++ "      type: "
    ++ columnTypeToYaml columnType

-- Function to convert Row to YAML string
rowToYaml :: Row -> String
rowToYaml row = "    - [" ++ intercalate ", " (map valueToYaml row) ++ "]"
