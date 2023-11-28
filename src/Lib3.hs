{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra (..),
  )
where

import Data.Yaml as Yaml
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Vector as V
import qualified Data.Text as T
import Control.Monad.Free (Free (..), liftF)
import Data.List (find, isPrefixOf, intercalate, dropWhileEnd, delete)
import Data.Time (UTCTime)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import Lib2
import Data.Char (toLower)
import Data.Maybe
import GHC.IO.Device (IODevice(close))

type TableName = String

type FileContent = String

-- In Lib3.hs
newtype Database = Database { unDatabase :: [(TableName, DataFrame)] }

type Table = (TableName, DataFrame)

type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile FilePath (FileContent -> next)
  | SaveFile FilePath FileContent next
  | DeleteFile FilePath next
  | RenameFile FilePath FilePath next
  | GetTime (UTCTime -> next)
  deriving (Functor)

data YamlData = YamlData { tables :: [YamlTable] } deriving (Show)
data YamlTable = YamlTable { name :: String } deriving (Show)

type Execution = Free ExecutionAlgebra

instance FromJSON Database where
  parseJSON = parseDatabase

loadFile :: FilePath -> Execution FileContent
loadFile path = liftF $ LoadFile path id

saveFile :: FilePath -> FileContent -> Execution ()
saveFile path content = liftF $ SaveFile path content ()

deleteFile :: FilePath -> Execution ()
deleteFile path = liftF $ DeleteFile path ()

renameFile :: FilePath -> FilePath -> Execution ()
renameFile pathSrc pathDst = liftF $ RenameFile pathSrc pathDst ()

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
  Left err -> return $ Left err
  Right statement -> do
    employeeTableContents <- loadFile "db/tables.yaml"
    let generatedDatabase = yamlToDatabase employeeTableContents
    case statement of
      LoadDatabase -> do
        let result = executeStatement statement (unDatabase generatedDatabase)
        return $ case result of
          Left err -> Left $ databaseToYaml generatedDatabase ++ "\n" ++ err
          Right df -> Right df
      SaveDatabase path -> do
        let result = executeStatement statement (unDatabase generatedDatabase)
        saveFile "db/tablesTemp.yaml" (databaseToYaml generatedDatabase)
        deleteFile path
        renameFile "db/tablesTemp.yaml" path
        return $ case result of
          Left err -> Left err
          Right df -> Right df
      (Now columnNames tableName maybeOperator) -> do
        currentTime <- getTime
        return $ Right $ DataFrame [Column "CurrentTime" StringType] [[StringValue (show currentTime)]]
      _ -> do
        let result = executeStatement statement (unDatabase generatedDatabase)
        return $ case result of
          Left err -> Left err
          Right df -> Right df



-- TODO Convert Database to Yaml string
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



-- TODO Convert Database to YAML string
databaseToYaml :: Database -> String
databaseToYaml (Database tables) = unlines $ map tableToYaml tables


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
valueToYaml (StringValue str) = str
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
