{-# OPTIONS_GHC -Wno-dodgy-imports #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use lambda-case" #-}

module Lib2
  ( parseStatement,
    getColumnName,
    executeStatement,
    Operator (..),
    ParsedStatement (..),
  )
where

import Data.Char (isSpace, toLower)
import Data.List (elemIndex, find, findIndex, isPrefixOf, intercalate)
import Data.Maybe (fromJust, fromMaybe)
import DataFrame (Column (..), ColumnType (..), DataFrame (..), Row (..), Value (..))
import InMemoryTables (TableName)
import Lib1 ()
import Text.Read (readMaybe)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

type FileContent = String

data Operator = Operator String String Value deriving (Show, Eq)

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTables
  | ShowTable TableName
  | Select [String] [TableName] (Maybe [Operator]) (Maybe String)
  | LoadDatabase
  | SaveDatabase FilePath
  | Update TableName [(String, Value)] (Maybe [Operator])
  | Insert TableName [String]
  | Delete TableName (Maybe [Operator])
  | Now [String] TableName (Maybe [Operator])
  | ParsedStatement
  | Where [Operator]
  deriving (Show, Eq)

instance Ord Value where
  compare (StringValue s1) (StringValue s2) = compare s1 s2
  compare _ _ = EQ -- Handle other Value constructors, e.g., handle Null or other types

parseStatement :: String -> String -> Either ErrorMessage ParsedStatement
parseStatement input getTime
  | null input = Left "Empty input"
  | last input == ';' = parseStatement (init input) getTime
  | otherwise =
      let parseableList = replaceKeywordsToLower (splitStringIntoWords input)
       in case parseableList of
            ["show", "tables"] -> Right ShowTables
            ["show", "table", table] -> Right (ShowTable table)
            ("select" : columns) ->
              case break (== "from") columns of
                (cols, "from" : rest) ->
                  let splitCols = splitCommaSeparated (unwords cols)
                      (tableNames, conditionsPart) = break (== "where") rest
                      tableNameList = splitCommaSeparated (unwords tableNames)
                   in case conditionsPart of
                        ("where" : whereClause) ->
                          if null whereClause
                            then Left "Invalid WHERE statement"
                            else
                          let (joinCondition, remainingWhereClause) =
                                if length tableNames > 1
                                then parseJoinCondition whereClause
                                else (Nothing, whereClause)
                          in if null remainingWhereClause
                                then Right (Select splitCols tableNameList Nothing joinCondition)
                                else do
                                  (conditions, _) <- parseWhereConditions remainingWhereClause getTime
                                  if null conditions
                                    then Left "Invalid WHERE statement"
                                    else Right (Select splitCols tableNameList (Just conditions) joinCondition)
                        _ -> Right (Select splitCols tableNameList Nothing Nothing)
                _ -> Left "Invalid SELECT statement"
            "update" : table : rest ->
              case dropWhile (/= "set") rest of
                "set" : assignments -> do
                  (values, remaining) <- parseUpdateAssignments assignments
                  (conditions, _) <- parseWhereConditions remaining getTime
                  Right (Update table values (Just conditions))
                _ -> Left "Invalid UPDATE statement"
            "insert" : "into" : tableName : rest ->
              case break (== "values") rest of
                (_, "values" : values) ->
                  let valueList = splitCommaSeparated (unwords values)
                  in Right (Insert tableName valueList)
                _ -> Left "Invalid INSERT statement"
            "delete" : "from" : tableName : rest ->
              case break (== "where") rest of
                (_, "where" : conditions) -> do
                  (parsedConditions, _) <- parseWhereConditions conditions
                  Right (Delete tableName (Just parsedConditions))
                _ -> Left "Invalid DELETE statement"
            _ -> Left "Not supported statement"




splitCommaSeparated :: String -> [String]
splitCommaSeparated str = map trimWhitespace $ splitOnComma str
  where
    splitOnComma = words . map (\c -> if c == ',' then ' ' else c)

parseJoinCondition :: [String] -> (Maybe String, [String])
parseJoinCondition whereClause =
  case whereClause of
    (col1 : "=" : col2 : rest) ->
      if head col2 == '\"' && last col2 == '\"'
        then (Nothing, whereClause)
        else (Just (unwords [col1, "=", col2]), rest)
    _ -> (Nothing, whereClause)

replaceKeywordsToLower :: [String] -> [String]
replaceKeywordsToLower = map replaceKeyword
  where
    replaceKeyword :: String -> String
    replaceKeyword keyword
      | map toLower keyword `elem` keywordsList = map toLower keyword
      | otherwise = keyword

keywordsList :: [String]
keywordsList = ["show", "table", "tables", "select", "from", "where", "and", "or", "not", "now", "min", "sum", "update", "set", "insert", "into", "values", "delete"]

toLowerPrefix :: String -> String -> Bool
toLowerPrefix prefix str = map toLower prefix `isPrefixOf` map toLower str

parseValue :: String -> Either ErrorMessage Value
parseValue s
  | head s == '\"' = Right $ StringValue (init (tail s))
  | otherwise =
      case readMaybe s of
        Just intValue -> Right (IntegerValue intValue)
        Nothing ->
          case map toLower s of
            "true" -> Right (BoolValue True)
            "false" -> Right (BoolValue False)
            "null" -> Right NullValue
            _ -> Left $ "Invalid value" ++ s

parseUpdateAssignments :: [String] -> Either ErrorMessage ([(String, Value)], [String])
parseUpdateAssignments [] = Left "Invalid UPDATE statement"
parseUpdateAssignments ("where" : rest) = Right ([], rest)
parseUpdateAssignments (colName : "=" : value : rest) = do
  (assignments, remaining) <- parseUpdateAssignments rest
  parsedValue <- parseValue value
  Right ((colName, parsedValue) : assignments, remaining)
parseUpdateAssignments _ = Left "Invalid UPDATE statement"

parseWhereConditions :: [String] -> String -> Either ErrorMessage ([Operator], [String])
parseWhereConditions [] _ = Right ([], [])
parseWhereConditions ("and" : rest) getTime
  | null rest = Left "Incomplete AND statement"
  | otherwise = do
      (operators, remaining) <- parseWhereConditions rest getTime
      Right (operators, remaining)

parseWhereConditions (colName : op : value : rest) getTime
  | not (null rest) && head rest /= "and" = Left "Invalid WHERE statement"
  | op `notElem` ["=", "/=", "<>", "<", ">", "<=", ">="] = Left "Invalid operator"
  | head value == '\"' && last value == '\"' = do
    let stringValue = init (tail value)
    parseOperator colName op (StringValue stringValue) rest
  | value `toLowerPrefix` "now()" = parseOperator colName op (StringValue getTime) rest
  | otherwise = case readMaybe value of
    Just intValue -> parseOperator colName op (IntegerValue intValue) rest
    Nothing -> parseNonNumericOperator colName op value rest
  where
    parseOperator col op val remaining = do
      (operators, remaining') <- parseWhereConditions remaining getTime
      Right (Operator col op val : operators, remaining')

    parseNonNumericOperator col op val remaining = case op of
      "=" -> parseEqualityOperator col val remaining
      "/=" -> parseInequalityOperator col val remaining
      "<>" -> parseInequalityOperator col val remaining
      _ -> Left "Invalid operator for non-numeric value"

    parseEqualityOperator col val remaining = case val of
      "True" -> parseOperator col "=" (BoolValue True) remaining
      "False" -> parseOperator col "=" (BoolValue False) remaining
      "NULL" -> parseOperator col "=" NullValue remaining
      _ | isLikelyColumnName val -> parseOperator col "=" (StringValue val) remaining
        | otherwise -> Left "Invalid value for equality operator"

    parseInequalityOperator col val remaining = case val of
      "True" -> parseOperator col "/=" (BoolValue True) remaining
      "False" -> parseOperator col "/=" (BoolValue False) remaining
      "NULL" -> parseOperator col "/=" NullValue remaining
      _ | isLikelyColumnName val -> parseOperator col "/=" (StringValue val) remaining
        | otherwise -> Left "Invalid value for inequality operator"

parseWhereConditions _ _ = Right ([], [])


collectStringValue :: [String] -> (String, [String])
collectStringValue [] = ("", [])
collectStringValue (x : xs)
  | last x == '\"' = (x, xs)
  | otherwise = let (value, remaining) = collectStringValue xs in (x ++ " " ++ value, remaining)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- ExecuteStatement function
executeStatement :: ParsedStatement -> Database -> String -> Either ErrorMessage DataFrame
executeStatement ShowTables database _ = Right $ showTables database
executeStatement (ShowTable tableName) database _ =
  case lookup tableName database of
    Just df -> Right $ DataFrame [Column "columns" StringType] (map (\col -> [StringValue (getColumnName col)]) (getColumns df))
    Nothing -> Left "Table not found"
executeStatement (Select columnNames tableNames maybeOperator maybeJoinCondition) database getTime
  | null columnNames = Left "No columns provided"
  | null tableNames = Left "No table provided"
  | ("min(" `toLowerPrefix` head columnNames || "sum(" `toLowerPrefix` head columnNames) && not (null (tail columnNames)) = Left "Cannot use aggregate functions with multiple columns"
  | otherwise = case mapM (`lookup` database) tableNames of
      Just dfs -> do
        let mergedDF = mergeDataFrames tableNames dfs
        let finalDF = case maybeJoinCondition of
              Just joinCondition -> joinTablesOnCondition mergedDF joinCondition
              Nothing -> mergedDF
        let pureColumnNames = map extractColumnNameFromFunction columnNames
        let missingColumns = filter (\colName -> not (any (\col -> getColumnName col == colName) (getColumns finalDF))) pureColumnNames
        if not (null missingColumns) && head pureColumnNames /= "*" && head pureColumnNames /= "now()"
          then Left $ "Column(-s) not found: " ++ unwords missingColumns
          else Right ()
        if head pureColumnNames == "now()" && getTime == "No current time. You have to run \"stack run fp2023-manipulate\" to use the agregate function \"now()\"."
          then Left getTime
          else Right ()
        let filteredDataFrame = case maybeOperator of
              Just operators -> filterDataFrameByOperators operators finalDF
              Nothing -> finalDF
        let filteredRows = getRows filteredDataFrame
        let colIndex =
              if "min(" `toLowerPrefix` head columnNames || "sum(" `toLowerPrefix` head columnNames
                then columnIndex finalDF (Column (head columnNames) StringType)
                else -1
        let selectedCols
              | "min(" `toLowerPrefix` head columnNames && length columnNames == 1 = [Column "minimum" (columnType (getColumns finalDF !! colIndex))]
              | "sum(" `toLowerPrefix` head columnNames && length columnNames == 1 = [Column "sum" (columnType (getColumns finalDF !! colIndex))]
              | "now()" `toLowerPrefix` head columnNames && length columnNames == 1 = [Column "current_time" StringType]
              | "*" `elem` columnNames = getColumns finalDF
              | otherwise = filter (\col -> getColumnName col `elem` columnNames) (getColumns finalDF)
        let selectedIndices = map (columnIndex finalDF) selectedCols
        let minValResult = case filteredRows of
              [] -> NullValue
              _ -> foldl1 (minValue colIndex (columnType (getColumns finalDF !! colIndex))) (map (!! colIndex) filteredRows)
        let selectedRows
              | getColumnName (head selectedCols) == "minimum" = [[minValResult]]
              | getColumnName (head selectedCols) == "sum" = [[calculateSum colIndex filteredRows]]
              | getColumnName (head selectedCols) == "current_time" = [[StringValue getTime]]
              | otherwise = map (\row -> map (row !!) selectedIndices) filteredRows
        Right $ DataFrame selectedCols selectedRows
      Nothing -> Left "Table(-s) not found"
executeStatement (Update tableName values maybeConditions) database getTime = do
  existingTable <- maybeTableToEither (lookup tableName database)
  let updatedRows = updateRows values maybeConditions (getRows existingTable)
  Right $ DataFrame (getColumns existingTable) updatedRows
executeStatement (Insert tableName values) database getTime = do
  existingTable <- maybeTableToEither (lookup tableName database)
  let newRows = map (\row -> row ++ map StringValue values) (getRows existingTable)
  Right $ DataFrame (getColumns existingTable) newRows
executeStatement (Delete tableName maybeConditions) database getTime = do
  existingTable <- maybeTableToEither (lookup tableName database)
  let deletedRows = case maybeConditions of
        Just conditions -> filter (evalConditionOnRow conditions existingTable) (getRows existingTable)
        Nothing -> []
  let remainingRows = filter (`notElem` deletedRows) (getRows existingTable)
  Right $ DataFrame (getColumns existingTable) remainingRows
executeStatement _ _ _ = Left "Not implemented"

dataframeToString :: DataFrame -> String
dataframeToString df =
  unlines $
    map
      ( intercalate ", " . map
              ( \col ->
                  case col of
                    StringValue s -> "\"" ++ s ++ "\""
                    IntegerValue i -> show i
                    BoolValue b -> show b
                    NullValue -> "null"
              )
      )
      (getRows df)

updateRows :: [(String, Value)] -> Maybe [Operator] -> [Row] -> [Row]
updateRows _ _ [] = []
updateRows values maybeConditions (row : rows) =
  let existingTable = DataFrame (getColumns existingTable) [row]
   in if all (\(Operator colName op val) -> evalCondition colName op val existingTable row) (fromMaybe [] maybeConditions)
        then updateRow values existingTable row : updateRows values maybeConditions rows
        else row : updateRows values maybeConditions rows

updateRow :: [(String, Value)] -> DataFrame -> Row -> Row
updateRow [] _ row = row
updateRow ((colName, newValue) : rest) existingTable row =
  case findIndex (\(Column name _) -> name == colName) (getColumns existingTable) of
    Just index -> updateRow rest existingTable (take index row ++ [newValue] ++ drop (index + 1) row)
    Nothing -> updateRow rest existingTable row

evalConditionOnRow :: [Operator] -> DataFrame -> Row -> Bool
evalConditionOnRow conditions df row = all (\cond -> evalConditionOnRow' cond df row) conditions

evalConditionOnRow' :: Operator -> DataFrame -> Row -> Bool
evalConditionOnRow' (Operator colName op val) = evalCondition colName op val

mergeDataFrames :: [TableName] -> [DataFrame] -> DataFrame
mergeDataFrames tableNames dfs
  | length dfs == 1 = head dfs
  | otherwise =
      let prefixedDataFrames = zipWith prefixDataFrame tableNames dfs
          mergedDataFrame = foldl1 mergeTwoDataFrames prefixedDataFrames
       in mergedDataFrame

prefixDataFrame :: TableName -> DataFrame -> DataFrame
prefixDataFrame tableName df =
  let prefixColumn (Column name colType) = Column (tableName ++ "." ++ name) colType
      prefixedColumns = map prefixColumn (getColumns df)
   in DataFrame prefixedColumns (getRows df)

mergeTwoDataFrames :: DataFrame -> DataFrame -> DataFrame
mergeTwoDataFrames df1 df2 =
  let newColumns = getColumns df1 ++ getColumns df2
      newRows = [row1 ++ row2 | row1 <- getRows df1, row2 <- getRows df2]
   in DataFrame newColumns newRows

joinTablesOnCondition :: DataFrame -> String -> DataFrame
joinTablesOnCondition mergedDF joinCondition =
  let (maybeCondition, _) = parseJoinCondition (words joinCondition)
      (col1, col2) = case maybeCondition of
        Just cond -> parseJoinConditionParts cond
        Nothing -> error "Invalid join condition format"
      filteredRows = filterRowsByJoinCondition mergedDF col1 col2
      col2Index = columnIndex mergedDF (Column col2 StringType)
      columnsWithoutRedundant = removeRedundantColumn (getColumns mergedDF) col2
      adjustedRows = map (removeElementAt col2Index) filteredRows
   in DataFrame columnsWithoutRedundant adjustedRows

removeElementAt :: Int -> [a] -> [a]
removeElementAt idx xs = let (left, right) = splitAt idx xs in
  case right of
    [] -> left
    (_ : rest) -> left ++ rest

removeRedundantColumn :: [Column] -> String -> [Column]
removeRedundantColumn columns colToRemove =
  filter (\(Column name _) -> name /= colToRemove) columns

parseJoinConditionParts :: String -> (String, String)
parseJoinConditionParts condition =
  case words condition of
    [col1, "=", col2] -> (col1, col2)
    _ -> error "Invalid join condition format"

filterRowsByJoinCondition :: DataFrame -> String -> String -> [Row]
filterRowsByJoinCondition df col1Name col2Name =
  let col1Index = columnIndex df (Column col1Name StringType) -- Modify as per your ColumnType
      col2Index = columnIndex df (Column col2Name StringType)
      filteredRows = filter (\row -> row !! col1Index == row !! col2Index) (getRows df)
   in filteredRows

extractColumnNameFromFunction :: String -> String
extractColumnNameFromFunction columnName
  | "min(" `toLowerPrefix` columnName = drop 3 (init (tail columnName))
  | "sum(" `toLowerPrefix` columnName = drop 3 (init (tail columnName))
  | otherwise = columnName

-- Helper functions
showTables :: Database -> DataFrame
showTables database = DataFrame [Column "tables" StringType] (map (return . StringValue . fst) database)

maybeTableToEither :: Maybe DataFrame -> Either ErrorMessage DataFrame
maybeTableToEither (Just df) = Right df
maybeTableToEither Nothing = Left "Table not found"

maybeFilterRows :: DataFrame -> Maybe Operator -> Maybe [Row]
maybeFilterRows df maybeOperator = case maybeOperator of
  Just (Operator colName op compCase) -> Just $ filter (evalCondition colName op compCase df) (getRows df)
  Nothing -> Just $ getRows df

filterColumns :: [String] -> DataFrame -> [Column]
filterColumns columnNames df = filter (\col -> getColumnName col `elem` columnNames) (getColumns df)

selectRow :: DataFrame -> [Column] -> Row -> Row
selectRow df selectedCols row = map (\col -> row !! columnIndex df col) selectedCols

evalCondition :: String -> String -> Value -> DataFrame -> Row -> Bool
-- evalCondition "and" colName val df row = any (\cond -> evalCondition cond df row) conditions
evalCondition colName "=" val df row =
  case val of
    StringValue otherColName
      | isLikelyColumnName otherColName ->
          let val1 = getColumnValue colName df row
              val2 = getColumnValue otherColName df row
           in val1 == val2
      | otherwise -> matchValue (getColumnValue colName df row) val
    _ -> matchValue (getColumnValue colName df row) val
evalCondition colName "/=" val df row = not (matchValue (getColumnValue colName df row) val)
evalCondition colName "<>" val df row = not (matchValue (getColumnValue colName df row) val)
evalCondition colName "<" val df row = compareValues (<) colName val df row
evalCondition colName ">" val df row = compareValues (>) colName val df row
evalCondition colName "<=" val df row = compareValues (<=) colName val df row
evalCondition colName ">=" val df row = compareValues (>=) colName val df row
evalCondition _ _ _ _ _ = False

columnType :: Column -> ColumnType
columnType (Column _ colType) = colType

filterDataFrameByOperators :: [Operator] -> DataFrame -> DataFrame
filterDataFrameByOperators operators df =
  foldr filterDataFrameByOperator df operators

filterDataFrameByOperator :: Operator -> DataFrame -> DataFrame
filterDataFrameByOperator (Operator colName op compCase) = filterDataFrameByColumnValue colName op compCase

-- Function to filter a DataFrame based on a specified column value
filterDataFrameByColumnValue :: String -> String -> Value -> DataFrame -> DataFrame
filterDataFrameByColumnValue colName op val df = do
  let filteredRows = filter (evalCondition colName op val df) (getRows df)
  DataFrame (getColumns df) filteredRows {- matchValue (getColumnValue colName df row) val) (getRows df) -}

getColumn :: String -> DataFrame -> Column
getColumn colName df = case find (\col -> getColumnName col == colName) (getColumns df) of
  Just col -> col
  Nothing -> error "Column not found"

-- Helper function to find the column index by name
findColumnIndex :: String -> [Column] -> Maybe Int
findColumnIndex colName cols = elemIndex colName (map getColumnName cols)

matchValue :: Value -> Value -> Bool
matchValue (StringValue s1) (StringValue s2) = s1 == s2
matchValue (IntegerValue i1) (IntegerValue i2) = i1 == i2
matchValue (BoolValue b1) (BoolValue b2) = b1 == b2
matchValue NullValue NullValue = True
matchValue _ _ = False

trimValue :: Value -> Value
trimValue (StringValue s) = StringValue (trimWhitespace s)
trimValue v = v

getColumnValue :: String -> DataFrame -> Row -> Value
getColumnValue colName df row =
  let colIndex = columnIndex df (Column (trimWhitespace (map toLower colName)) StringType)
   in row !! colIndex

trimWhitespace :: String -> String
trimWhitespace = filter (not . isSpace)

compareValues :: (String -> String -> Bool) -> String -> Value -> DataFrame -> Row -> Bool
compareValues op colName val df row =
  case (trimValue (getColumnValue colName df row), trimValue val) of
    (StringValue s1, StringValue s2) -> op (map toLower s1) (map toLower s2)
    (IntegerValue i1, IntegerValue i2) -> op (show i1) (show i2)
    _ -> False

getColumnName :: Column -> String
getColumnName (Column name _)
  | toLowerPrefix "min(" name = init $ tail $ dropWhile (/= '(') name
  | toLowerPrefix "sum(" name = init $ tail $ dropWhile (/= '(') name
  | "now()" `toLowerPrefix` name = name
getColumnName (Column name _) = name

columnIndex :: DataFrame -> Column -> Int
columnIndex (DataFrame cols _) col = case find (\c -> getColumnName c == getColumnName col) cols of
  Just foundCol -> fromJust $ elemIndex foundCol cols
  -- if Nothing then return error "Column not found" and the name of the column that was not found
  Nothing -> error $ "Column not found: " ++ getColumnName col

getRows :: DataFrame -> [Row]
getRows (DataFrame _ rs) = rs

getColumns :: DataFrame -> [Column]
getColumns (DataFrame cols _) = cols

minVal :: String -> DataFrame -> Maybe Value
minVal colName (DataFrame cols rows) = do
  colIndex <- findColumnIndex colName cols
  let colValues = map (!! colIndex) rows
  case colValues of
    [] -> Nothing -- Empty column
    _ -> Just (minimum colValues)

minValue :: Int -> ColumnType -> Value -> Value -> Value
minValue _ IntegerType (IntegerValue i1) (IntegerValue i2) = if i1 < i2 then IntegerValue i1 else IntegerValue i2
minValue _ BoolType (BoolValue b1) (BoolValue b2) = if b1 < b2 then BoolValue b1 else BoolValue b2
minValue _ StringType (StringValue s1) (StringValue s2) = if s1 < s2 then StringValue s1 else StringValue s2
minValue _ _ val1 NullValue = val1
minValue _ _ NullValue val2 = val2
minValue colIndex colType _ _ = error $ "Column type mismatch for column at index " ++ show colIndex ++ ". Expected " ++ show colType

calculateSum :: Int -> [Row] -> Value
calculateSum colIndex rows =
  let totalSum = sum [case row !! colIndex of IntegerValue i -> i; _ -> 0 | row <- rows]
      count = toInteger (length rows)
   in if count > 0 then IntegerValue totalSum else NullValue

splitStringIntoWords :: String -> [String]
splitStringIntoWords = words

getKeywordCaseSensitive :: String -> [String] -> String
getKeywordCaseSensitive keyword (x : xs) = if map toLower x == keyword then x else getKeywordCaseSensitive keyword xs
getKeywordCaseSensitive _ [] = []

splitSQL :: String -> String -> (String, String)
splitSQL keyword input = case breakOnCaseInsensitive keyword input of
  Just (select, rest) -> (select, rest)
  Nothing -> ("", input)

breakOnCaseInsensitive :: String -> String -> Maybe (String, String)
breakOnCaseInsensitive _ [] = Nothing
breakOnCaseInsensitive toFind text@(t : ts) =
  if isPrefixCaseInsensitive toFind text
    then Just (text, "")
    else case breakOnCaseInsensitive toFind ts of
      Just (before, after) -> Just (t : before, after)
      Nothing -> Nothing

isPrefixCaseInsensitive :: String -> String -> Bool
isPrefixCaseInsensitive [] _ = True
isPrefixCaseInsensitive _ [] = False
isPrefixCaseInsensitive (a : as) (b : bs) = toLower a == toLower b && isPrefixCaseInsensitive as bs

isLikelyColumnName :: String -> Bool
isLikelyColumnName str = not (any (`elem` "\"0123456789") str) && '.' `elem` str
