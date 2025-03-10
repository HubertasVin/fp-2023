import Control.Monad.Free (Free (..))
import Data.Either
import Data.IORef ()
import Data.Maybe ()
import Data.Time (fromGregorian)
import Data.Time.Clock
import DataFrame
import InMemoryTables qualified as D
import InMemoryTables qualified as DataFrame
import Lib1
import Lib2
import Lib3
import Lib3 (Database (..))
import System.Directory (doesFileExist, removeFile, renameFile, copyFile)
import System.IO (IOMode (ReadMode), hClose, withFile)
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
  describe "Lib2.parseStatement" $ do
    it "Parses SHOW TABLES statement" $ do
      Lib2.parseStatement "SHOW TABLES" "No current time." `shouldBe` Right ShowTables
    it "Parses SHOW TABLE statement" $ do
      Lib2.parseStatement "show table flags" "No current time." `shouldBe` Right (ShowTable "flags")
    it "Parses SELECT id FROM statement" $ do
      Lib2.parseStatement "SELECT id FROM employees" "No current time." `shouldBe` Right (Select ["id"] ["employees"] Nothing Nothing)
    it "Parses SELECT statement with case-sensitive columns and table names, ignoring SQL keyword case" $ do
      Lib2.parseStatement "SelecT Id FroM Employees" "No current time." `shouldBe` Right (Select ["Id"] ["Employees"] Nothing Nothing)
    it "Handles empty input" $ do
      Lib2.parseStatement "" "No current time." `shouldSatisfy` isLeft
    it "Handles not supported statements" $ do
      Lib2.parseStatement "wrong statement" "No current time." `shouldSatisfy` isLeft
    it "Handles empty select input" $ do
      Lib2.parseStatement "select" "No current time." `shouldSatisfy` isLeft
    it "Works with WHERE statements" $ do
      Lib2.parseStatement "select * from employees where id = 1" "No current time." `shouldBe` Right (Select ["*"] ["employees"] (Just [Operator "id" "=" (IntegerValue 1)]) Nothing)
    it "WHERE statements work with strings" $ do
      Lib2.parseStatement "select * from employees where name = \"Vi\"" "No current time." `shouldBe` Right (Select ["*"] ["employees"] (Just [Operator "name" "=" (StringValue "Vi")]) Nothing)
    it "Works with WHERE statements with multiple ANDs" $ do
      Lib2.parseStatement "select * from employees where id > 1 and name = \"Vi\" and surname = \"Po\"" "No current time." `shouldBe` Right (Select ["*"] ["employees"] (Just [Operator "id" ">" (IntegerValue 1), Operator "name" "=" (StringValue "Vi"), Operator "surname" "=" (StringValue "Po")]) Nothing)
    it "Handles where statement with incompatible operator" $ do
      Lib2.parseStatement "select * from employees where id is 1" "No current time." `shouldSatisfy` isLeft
    it "Handles incorrect where syntax" $ do
      Lib2.parseStatement "select * from employees where" "No current time." `shouldSatisfy` isLeft
    it "Handles update" $ do
      Lib2.parseStatement "update employees set id = 6 name = \"Ka\" surname = \"Mi\" where name = \"Vi\"" "No current time." `shouldBe` Right (Update "employees" [("id", IntegerValue 6), ("name", StringValue "Ka"), ("surname", StringValue "Mi")] (Just [Operator "name" "=" (StringValue "Vi")]))
    it "Handles insert" $ do
      Lib2.parseStatement "INSERT INTO employees (id, name, surname) VALUES (5, \"Alice\", \"Johnson\")" "No current time."  `shouldBe` Right (Insert "employees" [("id", IntegerValue 5), ("name", StringValue "Alice"), ("surname", StringValue "Johnson")])
    it "Handles delete" $ do
      Lib2.parseStatement "DELETE FROM employees WHERE id = 5" "No current time." `shouldSatisfy` isRight
  describe "Lib2.executeStatement" $ do
    it "Returns the SHOW TABLES dataframe correctly" $ do
      Lib2.executeStatement ShowTables DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "tables" StringType] [[StringValue "employees"], [StringValue "employeesSalary"], [StringValue "invalid1"], [StringValue "invalid2"], [StringValue "long_strings"], [StringValue "flags"]])
    it "Returns a SHOW TABLE dataframe correctly" $ do
      Lib2.executeStatement (ShowTable "employees") DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "columns" StringType] [[StringValue "id"], [StringValue "name"], [StringValue "surname"]])
    it "Handles not existing tables with SHOW TABLE" $ do
      Lib2.executeStatement (ShowTable "nothing") DataFrame.database "No current time." `shouldSatisfy` isLeft
    it "Returns a Dataframe with SELECT * correctly" $ do
      Lib2.executeStatement (Select ["*"] ["employees"] Nothing Nothing) DataFrame.database "No current time." `shouldBe` Right (snd D.tableEmployees)
    it "Returns a DataFrame with a SELECT with a specific column correctly" $ do
      Lib2.executeStatement (Select ["id"] ["employees"] Nothing Nothing) DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2], [IntegerValue 3], [IntegerValue 4]])
    it "Handles a SELECT statement with a nonexistent table" $ do
      Lib2.executeStatement (Select ["*"] ["nothing"] Nothing Nothing) DataFrame.database "No current time." `shouldSatisfy` isLeft
    it "Handles a SELECT statement with a nonexistent column" $ do
      Lib2.executeStatement (Select ["nothing"] ["employees"] Nothing Nothing) DataFrame.database "No current time." `shouldSatisfy` isLeft
    it "Returns a DataFrame with SELECT with where statement" $ do
      Lib2.executeStatement (Select ["*"] ["employees"] (Just [Operator "id" "=" (IntegerValue 1)]) Nothing) DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"]])
    it "Handles wrong select min syntax" $ do
      Lib2.executeStatement (Select ["min( id)"] ["employees"] Nothing Nothing) DataFrame.database "No current time." `shouldSatisfy` isLeft
    it "Returns a DataFrame with SELECT min correctly" $ do
      Lib2.executeStatement (Select ["min(id)"] ["employees"] Nothing Nothing) DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "minimum" IntegerType] [[IntegerValue 1]])
    it "Returns a DataFrame with SELECT sum with where case correctly" $ do
      Lib2.executeStatement (Select ["sum(id)"] ["employees"] (Just [Operator "id" ">" (IntegerValue 2)]) Nothing) DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "sum" IntegerType] [[IntegerValue 7]])
    it "Returns a DataFrame with SELECT with WHERE AND" $ do
      Lib2.executeStatement (Select ["*"] ["employees"] (Just [Operator "id" ">" (IntegerValue 1), Operator "name" "=" (StringValue "Ed")]) Nothing) DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    it "Returns a DataFrame with SELECT with WHERE with multiple AND statements" $ do
      Lib2.executeStatement (Select ["*"] ["employees"] (Just [Operator "id" ">" (IntegerValue 1), Operator "surname" "=" (StringValue "Dl"), Operator "name" "=" (StringValue "Ed")]) Nothing) DataFrame.database "No current time." `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
  describe "Testing Lib3" $ do

    it "Correctly serializing a yaml database" $ do
      yamlToDatabase databaseContent `shouldBe` Database {unDatabase = [("employees",DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"],[IntegerValue 3,StringValue "Hu",StringValue "Vi"],[IntegerValue 4,StringValue "Pa",StringValue "Dl"]]),("employeesSalary",DataFrame [Column "id" IntegerType,Column "salary" IntegerType] [[IntegerValue 1,IntegerValue 900],[IntegerValue 2,IntegerValue 300],[IntegerValue 3,IntegerValue 400],[IntegerValue 4,IntegerValue 1000]]),("flags",DataFrame [Column "flag" StringType,Column "value" BoolType] [[StringValue "a",BoolValue True],[StringValue "b",BoolValue True],[StringValue "b",NullValue],[StringValue "b",BoolValue False]]),("specimen",DataFrame [Column "isbn" IntegerType,Column "title" StringType,Column "author" StringType,Column "year" IntegerType,Column "taken" BoolType,Column "time_taken" StringType] [[IntegerValue 1,StringValue "The Hobbit (The Lord of the Rings, #0)",StringValue "J. R. R. Tolkien",IntegerValue 1937,BoolValue True,StringValue "2023-11-20 13:09:01.378811888 UTC"],[IntegerValue 2,StringValue "The Fellowship of the Ring (The Lord of the Rings, #1)",StringValue "J. R. R. Tolkien",IntegerValue 1954,BoolValue False,NullValue],[IntegerValue 3,StringValue "Pet Sematary",StringValue "Stephen King",IntegerValue 1983,BoolValue True,StringValue "2023-11-27 11:18:22.691749541 UTC"],[IntegerValue 4,StringValue "The Shining",StringValue "Stephen King",IntegerValue 1977,BoolValue True,StringValue "2023-11-18 15:43:02.232016126 UTC"]])]}
    it "Joining two tables" $ do
      result <- runExecuteIO $ Lib3.executeSql "select * from employees, employeesSalary where employees.id = employeesSalary.id;"
      result
        `shouldBe` Right
          ( DataFrame
              [Column "employees.id" IntegerType, Column "employees.name" StringType, Column "employees.surname" StringType, Column "employeesSalary.salary" IntegerType]
              [ [IntegerValue 1, StringValue "Vi", StringValue "Po", IntegerValue 900],
                [IntegerValue 2, StringValue "Ed", StringValue "Dl", IntegerValue 300],
                [IntegerValue 3, StringValue "Hu", StringValue "Vi", IntegerValue 400],
                [IntegerValue 4, StringValue "Pa", StringValue "Dl", IntegerValue 1000]
              ]
          )
    it "table with now() in columns" $ do
      result <- runExecuteIO $ Lib3.executeSql "select now() from flags;"
      case result of
        Right (DataFrame (column : _) _) -> getColumnName column `shouldBe` "current_time"
        _ -> error "Expected a DataFrame with at least one column"
    it "table comparing time in where" $ do
      result <- runExecuteIO $ Lib3.executeSql "select * from specimen where time_taken < \"2023-11-23\";"
      result
        `shouldBe` Right
          ( DataFrame
              [Column "isbn" IntegerType, Column "title" StringType, Column "author" StringType, Column "year" IntegerType, Column "taken" BoolType, Column "time_taken" StringType]
              [ [IntegerValue 1, StringValue "The Hobbit (The Lord of the Rings, #0)", StringValue "J. R. R. Tolkien", IntegerValue 1937, BoolValue True, StringValue "2023-11-20 13:09:01.378811888 UTC"],
                [IntegerValue 4, StringValue "The Shining", StringValue "Stephen King", IntegerValue 1977, BoolValue True, StringValue "2023-11-18 15:43:02.232016126 UTC"]
              ]
          )
    it "table comparing time in where with now()" $ do
      result <- runExecuteIO $ Lib3.executeSql "select * from specimen where time_taken < now();"
      result
        `shouldBe` Right
          ( DataFrame
              [Column "isbn" IntegerType, Column "title" StringType, Column "author" StringType, Column "year" IntegerType, Column "taken" BoolType, Column "time_taken" StringType]
              [ [IntegerValue 1, StringValue "The Hobbit (The Lord of the Rings, #0)", StringValue "J. R. R. Tolkien", IntegerValue 1937, BoolValue True, StringValue "2023-11-20 13:09:01.378811888 UTC"],
                [IntegerValue 3, StringValue "Pet Sematary", StringValue "Stephen King", IntegerValue 1983, BoolValue True, StringValue "2023-11-27 11:18:22.691749541 UTC"],
                [IntegerValue 4, StringValue "The Shining", StringValue "Stephen King", IntegerValue 1977, BoolValue True, StringValue "2023-11-18 15:43:02.232016126 UTC"]
              ]
          )
    it "update table with update statement " $ do
      result <- runExecuteIO $ Lib3.executeSql "update employees set id = 6 name = \"Ka\" surname = \"Mi\" where name = \"Vi\";"
      result `shouldBe` Right
        ( DataFrame
          [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
          [ [IntegerValue 6, StringValue "Ka", StringValue "Mi"],
            [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
            [IntegerValue 3, StringValue "Hu", StringValue "Vi"],
            [IntegerValue 4, StringValue "Pa", StringValue "Dl"]
          ]
        )
    it "insert into table with INSERT statement " $ do
      result <- runExecuteIO $ Lib3.executeSql "INSERT INTO employees (id, name, surname) VALUES (5, \"Al\", \"Jo\");"
      result `shouldBe` Right
        ( DataFrame
          [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
          [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
            [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
            [IntegerValue 3, StringValue "Hu", StringValue "Vi"],
            [IntegerValue 4, StringValue "Pa", StringValue "Dl"],
            [IntegerValue 5, StringValue "Al", StringValue "Jo"]
          ]
        )
    it "delete from table with DELETE statement " $ do
      result <- runExecuteIO $ Lib3.executeSql "DELETE FROM employees WHERE id = 2;"
      result `shouldBe` Right
        ( DataFrame
          [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
          [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
            [IntegerValue 3, StringValue "Hu", StringValue "Vi"],
            [IntegerValue 4, StringValue "Pa", StringValue "Dl"]
          ]
        )

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
  next <- runStep step
  runExecuteIO next
  where
    -- probably you will want to extend the interpreter
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = return fixedUTCTime >>= return . next
    runStep (Lib3.LoadFile next) = do
      return $ next databaseContent
    runStep (Lib3.SaveFile _ next) = do
      return next
    runStep (Lib3.RenameFile next) = do
      return next
    runStep (Lib3.CopyFile next) = do
      return next

fixedUTCTime :: UTCTime
fixedUTCTime = UTCTime (fromGregorian 2023 11 27) (secondsToDiffTime 43200) -- 2023-11-27 12:00:00

databaseContent :: String
databaseContent = "- table: employees\n  columns:\n    - name: id\n      type: IntegerType\n    - name: name\n      type: StringType\n    - name: surname\n      type: StringType\n  rows:\n    - [1, \"Vi\", \"Po\"]\n    - [2, \"Ed\", \"Dl\"]\n    - [3, \"Hu\", \"Vi\"]\n    - [4, \"Pa\", \"Dl\"]\n\n- table: employeesSalary\n  columns:\n    - name: id\n      type: IntegerType\n    - name: salary\n      type: IntegerType\n  rows:\n    - [1, 900]\n    - [2, 300]\n    - [3, 400]\n    - [4, 1000]\n\n- table: flags\n  columns:\n    - name: flag\n      type: StringType\n    - name: value\n      type: BoolType\n  rows:\n    - [\"a\", true]\n    - [\"b\", true]\n    - [\"b\", null]\n    - [\"b\", false]\n\n- table: specimen\n  columns:\n    - name: isbn\n      type: IntegerType\n    - name: title\n      type: StringType\n    - name: author\n      type: StringType\n    - name: year\n      type: IntegerType\n    - name: taken\n      type: BoolType\n    - name: time_taken\n      type: StringType\n  rows:\n    - [1, \"The Hobbit (The Lord of the Rings, #0)\", \"J. R. R. Tolkien\", 1937, true, \"2023-11-20 13:09:01.378811888 UTC\"]\n    - [2, \"The Fellowship of the Ring (The Lord of the Rings, #1)\", \"J. R. R. Tolkien\", 1954, false, null]\n    - [3, \"Pet Sematary\", \"Stephen King\", 1983, true, \"2023-11-27 11:18:22.691749541 UTC\"]\n    - [4, \"The Shining\", \"Stephen King\", 1977, true, \"2023-11-18 15:43:02.232016126 UTC\"]"