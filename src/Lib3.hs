{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}

module Lib3
  ( executeSql,
    getColumnName,
    yamlToDatabase,
    Database (..),
    Execution,
    ExecutionAlgebra (..),
  )
where

import Data.Yaml as Yaml
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Vector as V
import qualified Data.Text as T
import Control.Monad.Free (Free (..), liftF)
import Data.List (findIndex, intercalate)
import Data.Time (UTCTime)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row, Value (..))
import Lib2
import Data.Char (toLower)
import Data.Maybe
import GHC.IO.Device (IODevice(close))
import qualified InMemoryTables
import Data.Text (replace)
import Data.HashMap.Internal.Array (update)

type TableName = String

type FileContent = String

newtype Database = Database { unDatabase :: [(TableName, DataFrame)] } deriving (Show, Eq)

type Table = (TableName, DataFrame)

type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile (FileContent -> next)
  | SaveFile FileContent next
  | DeleteFile next
  | RenameFile next
  | CopyFile next
  | GetTime (UTCTime -> next)
  deriving (Functor)

data YamlData = YamlData { tables :: [YamlTable] } deriving (Show)
data YamlTable = YamlTable { name :: String } deriving (Show)

type Execution = Free ExecutionAlgebra

instance FromJSON Database where
  parseJSON = parseDatabase

loadFile :: Execution FileContent
loadFile = liftF $ LoadFile id

saveFile :: FileContent -> Execution ()
saveFile content = liftF $ SaveFile content ()

deleteFile :: Execution ()
deleteFile = liftF $ DeleteFile ()

renameFile :: Execution ()
renameFile = liftF $ RenameFile ()

copyFile :: Execution ()
copyFile = liftF $ CopyFile ()

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
  currentTime <- getTime
  case parseStatement sql (show currentTime) of
    Left err -> return $ Left err
    Right statement -> do
      tableContents <- loadFile
      let generatedDatabase = yamlToDatabase tableContents
      case statement of
        Update tableName values conditions -> do
          let existingTable = maybeTableToEither (lookup tableName (getTables generatedDatabase))
          case existingTable of
            Left _ -> return $ Left $ "Table not found: " ++ tableName
            Right df -> do
              let updatedRows = updateRows values df (fromJust conditions) (getRows df)
              case updatedRows of
                Left err -> return $ Left err
                Right updatedRows ->
                  let updatedDatabase = updateDatabaseWithDataFrame generatedDatabase (tableName, DataFrame (getColumns df) updatedRows)
                  in do
                  saveFile (databaseToYaml updatedDatabase)
                  renameFile
                  copyFile
                  return $ Right $ DataFrame (getColumns df) updatedRows
        Insert tableName values -> do
          let existingTable = maybeTableToEither (lookup tableName (getTables generatedDatabase))
          case existingTable of
            Left _ -> return $ Left $ "Table not found: " ++ tableName
            Right df -> do
              let insertedRow = insertRow values df
              case insertedRow of
                Left err -> return $ Left err
                Right insertedRow ->
                  let updatedDatabase = updateDatabaseWithDataFrame generatedDatabase (tableName, DataFrame (getColumns df) (getRows df ++ [insertedRow]))
                  in do
                  saveFile (databaseToYaml updatedDatabase)
                  renameFile
                  copyFile
                  return $ Right $ DataFrame (getColumns df) (getRows df ++ [insertedRow])
        Delete tableName conditions -> do
          let existingTable = maybeTableToEither (lookup tableName (getTables generatedDatabase))
          case existingTable of
            Left _ -> return $ Left $ "Table not found: " ++ tableName
            Right df -> do
              let updatedRows = removeRows (fromJust conditions) df
              case updatedRows of
                Left err -> return $ Left err
                Right updatedRows ->
                  let updatedDatabase = updateDatabaseWithDataFrame generatedDatabase (tableName, DataFrame (getColumns df) (getRows updatedRows))
                  in do
                  saveFile (databaseToYaml updatedDatabase)
                  renameFile
                  copyFile
                  return $ Right $ DataFrame (getColumns df) (getRows updatedRows)
        _ -> do
          let result = executeStatement statement (unDatabase generatedDatabase) (show currentTime)
          return $ case result of
            Left err -> Left err
            Right df -> Right df


-- Update database
updateDatabaseWithDataFrame :: Database -> Table -> Database
updateDatabaseWithDataFrame (Database tables) (tableName, dataFrame)
  | isNothing index = Database (tables ++ [(tableName, dataFrame)])
  | otherwise = Database (take (fromJust index) tables ++ [(tableName, dataFrame)] ++ drop (fromJust index + 1) tables)
  where
    index = findIndex (\(tableName1, _) -> tableName1 == tableName) tables


-- TODO Implement UPDATE statement
updateRows :: [(String, DataFrame.Value)] -> DataFrame -> [Operator] -> [Row] -> Either ErrorMessage [Row]
updateRows _ _ [] rows = Right rows
updateRows values df (condition:xs) rows = do
  updatedRows <- updateRows' values df condition rows
  updateRows values df xs updatedRows

updateRows' :: [(String, DataFrame.Value)] -> DataFrame -> Operator -> [Row] -> Either ErrorMessage [Row]
updateRows' _ _ (Operator {}) [] = Right []
updateRows' values df (Operator columnName operator value) (row : rows)
  | evalCondition columnName operator value df row = do
    updatedRow <- updateRow values df row
    updatedRows <- updateRows' values df (Operator columnName operator value) rows
    return $ updatedRow : updatedRows
  | otherwise = do
    updatedRows <- updateRows' values df (Operator columnName operator value) rows
    return $ row : updatedRows

updateRow :: [(String, DataFrame.Value)] -> DataFrame -> Row -> Either ErrorMessage Row
updateRow [] _ row = Right row
updateRow ((columnName, value):xs) df row =
  if isNothing index
    then Left $ "Column not found: " ++ columnName
    else case replaceValueByIndex (fromJust index) value row of
      Left errorMessage -> Left errorMessage
      Right updatedRow -> updateRow xs df updatedRow
  where
    index = getColumnIndex columnName df

getColumnIndex :: String -> DataFrame -> Maybe Int
getColumnIndex columnName df = findIndex (\(Column name _) -> name == columnName) (getColumns df)

replaceValueByIndex :: Int -> DataFrame.Value -> Row -> Either ErrorMessage Row
replaceValueByIndex index value row
  | index < 0 || index >= length row = Left $ "Index out of bounds: " ++ show index
  | not (compatibleTypes (row !! index) value) = Left $ "Incompatible value types: " ++ show (row !! index) ++ " " ++ show value
  | otherwise = Right (take index row ++ [value] ++ drop (index + 1) row)

-- Function to check if two values have compatible types
compatibleTypes :: DataFrame.Value -> DataFrame.Value -> Bool
compatibleTypes (IntegerValue _) (IntegerValue _) = True
compatibleTypes (StringValue _) (StringValue _) = True
compatibleTypes (BoolValue _) (BoolValue _) = True
compatibleTypes NullValue _ = True
compatibleTypes _ _ = False

getValueType :: DataFrame.Value -> ColumnType
getValueType (IntegerValue _) = IntegerType
getValueType (StringValue _) = StringType
getValueType (BoolValue _) = BoolType
getValueType NullValue = error "NullValue has no type"



-- TODO Implement INSERT statement
insertRow :: [(String, DataFrame.Value)] -> DataFrame -> Either ErrorMessage Row
insertRow values df = do
  let newRow = replicate (length (getColumns df)) NullValue
  updateRow values df newRow


getColumnsListLength :: [DataFrame.Column] -> Int
getColumnsListLength = foldr (\ x -> (+) 1) 0

getRowsListLength :: [Row] -> Int
getRowsListLength = foldr (\ x -> (+) 1) 0


-- TODO Implement DELETE statement
removeRows :: [Operator] -> DataFrame -> Either ErrorMessage DataFrame
removeRows conditions df = do
  updatedRows <- removeRows' (head conditions) df (getRows df)
  return $ DataFrame (getColumns df) updatedRows

removeRows' :: Operator -> DataFrame -> [Row] -> Either ErrorMessage [Row]
removeRows' _ _ [] = Right []
removeRows' (Operator columnName operator value) df (row : rows) =
  if evalCondition columnName operator value df row
    then removeRows' (Operator columnName operator value) df rows
    else do
      updatedRows <- removeRows' (Operator columnName operator value) df rows
      return $ row : updatedRows


-- TODO Convert Database to Yaml string
databaseToString :: Database -> String
databaseToString (Database tables) = unlines $ map outerTableToString tables
  where
    outerTableToString :: Table -> String
    outerTableToString (tableName, DataFrame columns rows) =
      unlines [innerTableToString (tableName, columns, rows)]
      where
        innerTableToString :: (String, [DataFrame.Column], [Row]) -> String
        innerTableToString (tableName1, columns1, rows1) =
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
databaseToYaml (Database tables) = unlines $ map outerTableToYaml tables

outerTableToYaml :: Table -> String
outerTableToYaml (tableName, DataFrame columns rows) =
  unlines [innerTableToYaml (tableName, columns, rows)]
  where
    innerTableToYaml :: (String, [DataFrame.Column], [Row]) -> String
    innerTableToYaml (tableName1, columns1, rows1) =
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



-- TODO Miscelaneous functions
getTables :: Database -> [(InMemoryTables.TableName, DataFrame)]
getTables = unDatabase