# SQL-Database-Engine



 It is resourceful to use this engine to simulate what happens under the hood of a real relational database engine when firing a query. 
 
 
 **If you use Eclipse IDE , make sure to import the file as a maven project** 

To recall what happens in a relational database visit [this article]( https://www.geeksforgeeks.org/relational-model-in-dbms/ ) 

```java 
public interface DBAppInterface {

    void init();

    void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType, Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException;

    void createIndex(String tableName, String[] columnNames) throws DBAppException;

    void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException;

    void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException;

    void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException;

    Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException;


}

```

## Accepted Data Types 
For simplicity , there are only 4 supported data types for column values in the engine. 

**java.lang.Integer , java.lang.String , java.lang.Double , java.util.Date**


## Read and Write Functionalities
The engine simulates reading and writing data from disk by loading and storing binary files into the projet directory. 

The serialize and deserialize methods are used to load and store the needed files that represent the tuples of the table

```java
void serialize(Object serlzObj, String path) {
		try {
			FileOutputStream fileOut = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(serlzObj);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	@SuppressWarnings("finally")
	Object deSerialize(String path) {
		Object out = null;
		try {
			FileInputStream fileIn = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			out = in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
			return null;
		} catch (ClassNotFoundException c) {
			c.printStackTrace();
			return null;
		} finally {
			return out;
		}

	}

```


## Tables and Pages 
To avoid loading entire tables into memory at runtime when performing a query, each table is divided into pages - where each page is a vector of tuples - and each page is stored and loaded as a binary file . To know which page holds a particular tuple , a virtual Table and virtual Page are used.

The virtual Page Class holds the ID of the page which represents the name of the actual page when it is stored on disk. The Min and Max Variables represent the range of values of the primary key of the table and the isFull Boolean indicates if the page is full depending on a preset maximumPageCapacity 
to also simulate different disk configurations. The maximum capacity of the page is set in the DBAPP.config file

```java
public class Page implements Serializable {
	protected String id;
	protected Object min;
	protected Object max;
	protected boolean isFull;
    private static final long serialVersionUID = 6529685098267757691L;


	public Page() {
		// TODO Auto-generated constructor stub
	}

	public Page(Object min, Object max, String id) {
		this.min = min;
		this.max = max;
		this.id = id;

	}
	
}

```
The virtual Table class organizes all the virtual pages in a "pages" vector , which will be used in queries to know the IDs of a particular page where a tuple should be inserted , deleted , updated , or selected based on the range of each virtual page. The engine solves the full Page issue by splitting the full page into 2 non full pages and updating the pages Vector and the ranges of the new pages.

```java 

public class Table implements Serializable {
    protected Vector<Page> pages;
    boolean noPages;
    String pk;
    String maxPageId;
    private static final long serialVersionUID = 6529685098267757690L;

	public Table() {
       pages = new Vector<Page>();
       noPages = true;
	}
	
	public void setNoPages(Boolean b)
	{
		this.noPages=b;

	}


	public String toString() {
		String s = "Primary key : " + pk + "\n" + "The table has no pages : " + noPages 
				+ "\n" + "Max Page ID : " + maxPageId + "\n";
		
		for( Page p : pages ) {
			s += " Page " +  p.id + " |" + "Min " + p.min + "|" + " Max " + p.max + " | Page Full  " + p.isFull;
			s+= "\n";
		}
		
		return s;
	}
	
}


```



## Methods
- [ Create Table ](#create-table)
- [ Insert into Table ](#insert-into-table)
- [ Delete From Table ](#delete-from-table)
- [ Create Index ](#create-index)
- [ Update Table ](#update-table)





# Create Table
*createTable* creates a new Table in the database and creates directory files to store future tuples.

*strClusteringKeyColumn* is the name of the column that will be the primary key and the clustering column as well.
 
 *htblColNameType* holds the column Names and Types 
  
 *htblColNameMin and htblColNameMax* hold the acceptable range of values for each

Incase that the table arleady exists or the column information are entered in correctly , a DBAppException is thrown 


```java
public void createTable(String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax )
throws DBAppException


String strTableName = "height2";
		String cluster = "id";

		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("date", "java.lang.Date");

		Hashtable htblColNameMin = new Hashtable();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "a");

		Date dob1 = new Date(1901 - 1900, 1 - 1, 1);
		htblColNameMin.put("date", "1901-01-01");

		Hashtable htblColNameMax = new Hashtable();
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name", "zzzzzzz");
		Date dob2 = new Date(2020 - 1900, 12 - 1, 31);

		htblColNameMax.put("date", "2020-12-30");

		try {
			a.createTable(strTableName, cluster, htblColNameType, htblColNameMin,
					htblColNameMax);

		} catch (DBAppException e) {
			e.printStackTrace();
		}

```



# Insert Into Table

*insertIntoTable* inserts a tuple into a particular page. 
Its side effects  includse creating new pages if the desired page or the table is empty.
*htblColNameValue* holds the values of the columns for the inserted tuples and must include a value for the primary key


```java

public void insertIntoTable(String strTableName,
Hashtable<String,Object> htblColNameValue)
throws DBAppException

		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", 3);
		htblColNameValue.put("name", "ahmed");
		Date dob3 = new Date(2010 - 1900, 5 - 1, 31);
		htblColNameValue.put("date", dob3);

		try {
			a.insertIntoTable("height2", htblColNameValue);


		} catch (DBAppException e) {
			e.printStackTrace();
		}
 ```


# Delete From Table

# Create Index 

# Update Table 

# Select From Table






