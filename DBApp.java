import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

//import jdk.vm.ci.meta.Local;

public class DBApp implements DBAppInterface {
	String globalPath;
	private int maxPageCount;
	String localPath = "\\src\\main\\resources\\data\\";

	public DBApp() {
		init();
	}

	@Override
	public void init() {

		Properties prop = new Properties();
		String fileName = "app.config";
		InputStream is = null;
		try {
			is = new FileInputStream(System.getProperty("user.dir") + "\\src\\main\\resources\\DBApp.config");
		} catch (FileNotFoundException ex) {
		}
		try {
			prop.load(is);
		} catch (IOException ex) {

		}
		maxPageCount = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		globalPath = System.getProperty("user.dir");

//		List<List<String>> l = new ArrayList<>();
//		ArrayList<String> tmp = new ArrayList<>();
//		tmp.add("");
//		l.add(tmp);
//		writeCSV( "/src/main/resources/tableNames.csv" , l , false);


	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
							Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

		ArrayList<String[]> tableNames = readCSV(globalPath + "\\src\\main\\resources\\tableNames.csv");

		for (String[] s : tableNames)
			for (String s1 : s) {
				if (s1.equals(tableName))
					throw new DBAppException("Table already exists");
			}

		Set cols = colNameType.keySet();
		Set min = colNameMin.keySet();
		Set max = colNameMax.keySet();

		if (!cols.containsAll(min) || !cols.containsAll(max))
			throw new DBAppException(" Missing/Invalid column metadata from one of the hashdata ");

		if (!min.containsAll(cols) || !min.containsAll(max))
			throw new DBAppException(" Missing/Invalid column metadata from one of the hashdata ");

		if (!max.containsAll(cols) || !max.containsAll(min))
			throw new DBAppException("Missing/Invalid column metadata from one of the hashdata ");

		if (!cols.contains(clusteringKey))
			throw new DBAppException("Clustering Key not included in Columns");

		List<List<String>> row = new ArrayList<List<String>>();
		for (Object col : cols) {
			String column = (String) col;

			ArrayList<String> s1 = new ArrayList<String>();
			s1.add(tableName);
			s1.add((column));
			// System.out.print(column + " ");

			s1.add(colNameType.get(column));
			// ((colNameType.get(column)));

			if (column.equals(clusteringKey)) {
				s1.add("True");
			} else {
				s1.add("False");
			}
			s1.add("False");
			s1.add(colNameMin.get(column));
			// ((colNameMin.get(column)));

			s1.add(colNameMax.get(column));
			// ((colNameMax.get(column)));

			row.add(s1);

		}

		writeCSV("/src/main/resources/metadata.csv", row);

		String[] tableNameString = { tableName };
		List<String> tableNameList = Arrays.asList(tableNameString);

		List<List<String>> tableNameRow = Arrays.asList(tableNameList);
		writeCSV("/src/main/resources/tableNames.csv", tableNameRow );

		String s = System.getProperty("user.dir");
		s += localPath;
		s += tableName;
		File test = new File(s);
		test.mkdir();

		Table newTable = new Table();
		newTable.pk = clusteringKey;
		String tablePath = "./src/main/resources/data/";
		tablePath += tableName + "/Table.class";
		try {
			FileOutputStream fileOut = new FileOutputStream(tablePath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(newTable);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}

	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {

	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub

		String tablePath = globalPath + "\\src\\main\\resources\\data\\" + tableName + "\\Table.class";

		ArrayList<String[]> tableNames = readCSV(globalPath + "\\src\\main\\resources\\tableNames.csv");

		ArrayList<String[]> metaDataArray = readCSV(globalPath + "\\src\\main\\resources\\metadata.csv");

		boolean tableExists = true;
		for (String[] s : tableNames)
			for (String s1 : s) {
				if (s1.equals(tableName)) {
					tableExists = false;
				}
			}

		if (tableExists) {
			throw new DBAppException("Table does not exist");
		}

		ArrayList<String[]> filteredArray = (ArrayList<String[]>) metaDataArray.stream()
				.filter(a -> a[0].equals(tableName)).collect(Collectors.toList());

		Set colNames = colNameValue.keySet();
		inputChecker(colNames, filteredArray, colNameValue);

		Table table = (Table) deSerialize(tablePath);

		if( !colNames.contains(table.pk) )
			throw new DBAppException(" Primary Key not specified. You must enter a valid value for primary key ");

		if (table.noPages) {
			firstPageRoutine(colNameValue, table, tableName);

		} else {
			// Go to Insert Method to find Page ID
			String pageID = insert(colNameValue.get(table.pk), table.pages);
			if (pageID.equals("-1")) {

				pageID = outOfRangeRoutine(colNameValue, table);

			}

			Page virtPage = null;
			for (Page p : table.pages) {
				if (p.id.equals(pageID))
					virtPage = p;
			}


			Vector page = ((Vector) deSerialize(globalPath + localPath + tableName + "\\" + pageID + ".class"));

			insertTuple(page, colNameValue, table.pk, virtPage, table, tableName, virtPage.id);
		}

	}

	void insertTuple(Vector<Hashtable<String, Object>> page, Hashtable<String, Object> colNameValue, String pk,
					 Page virtPage, Table table, String tableName, String virtPageId) throws DBAppException {

		// LinkedList tempPage = new LinkedList();
		int OldpageSize = page.size();
		int insertInHere = 0;
		for (int i = OldpageSize - 1; i >= 0; i--) {

			Hashtable<String, Object> tuple = (Hashtable<String, Object>) page.get(i);
			// (colNameValue.get("id"));
			if (compare(tuple.get(pk), colNameValue.get(pk)) == 0)
				throw new DBAppException("Tuple arleady exists in Table");
			else if (compare(tuple.get(pk), colNameValue.get(pk)) < 0) {
				insertInHere = i + 1;
				break;
//			} else {
//				tempPage.addFirst(page.remove(i));
//			}
			}
		}
		page.add(insertInHere, colNameValue);

//		for (Hashtable<String, Object> h : page)
//			System.out.print(h.get(pk) + " ");
//
//		();

		Vector<Hashtable<String, Object>> newPage = new Vector();
		if (OldpageSize == maxPageCount) {
			int j = (OldpageSize + 1) / 2;
			for (int i = 0; i < OldpageSize / 2 + 1; i++)
				newPage.add(page.remove(j));

			// page : old page
			// new page: new page
			// grab maxPage
			String maxId = table.maxPageId;
			int maxIdInt = Integer.parseInt(maxId) + 1;
			table.maxPageId = maxIdInt + "";
			// create new Page
			Page newVirtPage = new Page(((Hashtable) newPage.get(0)).get(pk),
					((Hashtable) newPage.get(newPage.size() - 1)).get(pk), table.maxPageId);

			// update old page
			virtPage.min = ((Hashtable) page.get(0)).get(pk);
			virtPage.max = ((Hashtable) page.get(page.size() - 1)).get(pk);
			virtPage.isFull = false;

			table.pages.add(table.pages.indexOf(virtPage) + 1, newVirtPage);

			serialize(table, globalPath + "\\src\\main\\resources\\data\\" + tableName + "\\" + "Table" + ".class");
			serialize(page, globalPath + "\\src\\main\\resources\\data\\" + tableName + "\\" + virtPageId + ".class");
			serialize(newPage,
					globalPath + "\\src\\main\\resources\\data\\" + tableName + "\\" + table.maxPageId + ".class");

		}

		else {
			if (OldpageSize == maxPageCount - 1) {
				virtPage.isFull = true;
			}
			virtPage.min = ((Hashtable) page.get(0)).get(pk);
			virtPage.max = ((Hashtable) page.get(page.size() - 1)).get(pk);

			serialize(table, globalPath + localPath + tableName + "\\" + "Table" + ".class");
			serialize(page, globalPath + localPath + tableName + "\\" + virtPageId + ".class");

		}
		// update virtual old page and new if it exists

//		for (Object u : page) {
//
//			Hashtable<String, Object> h = (Hashtable<String, Object>) u;
//
//			System.out.print(h.get(pk) + " ");
//		}
//
//		();
//
//		for (Object u : newPage) {
//
//			Hashtable<String, Object> h = (Hashtable<String, Object>) u;
//
//			System.out.print(h.get(pk) + " ");
//		}

	}

	void updateVirtTable(Vector page, Page virtPage, Table table, boolean newPage, String virtPageId,
						 int virtPageIndex) {

		virtPage.max = ((Hashtable<String, Object>) page.get(page.size() - 1)).get(table.pk);
		virtPage.min = ((Hashtable<String, Object>) page.get(0)).get(table.pk);
		if (page.size() == maxPageCount)
			virtPage.isFull = true;

		if (newPage) {
			//
		}

	}

	String outOfRangeRoutine(Hashtable<String, Object> colNameValue, Table table) {
		String pageID = "";
		if (compare(colNameValue.get(table.pk), table.pages.get(0).min) < 0) {
			pageID = "1";
		} else {
			// Traverse pages from end to start until finding a page that has a max less
			// than the PK. Go to that page
			for (int i = table.pages.size() - 1; i >= 0; i--) {
				if (compare(table.pages.get(i).max, colNameValue.get(table.pk)) < 0) {
					if (i != table.pages.size() - 1 && table.pages.get(i).isFull && !table.pages.get(i + 1).isFull)
						pageID = table.pages.get(i + 1).id;
					else
						pageID = table.pages.get(i).id;

					break;
				}
			}
		}

		return pageID;
	}

	int compare(Object a, Object b) {

		if (a instanceof String) {
			return ((String) a).compareTo((String) b);
		} else if (a instanceof Date) {
			return ((Date) a).compareTo((Date) b);
		} else if (a instanceof Double) {
			return ((Double) a).compareTo((Double) b);
		} else {
			return ((Integer) a).compareTo((Integer) b);
		}
	}

	void inputChecker(Set colNames, ArrayList<String[]> filteredArray, Hashtable<String, Object> colNameValue)
			throws DBAppException {
		boolean flag = false;

		ArrayList<String> s1 = new ArrayList<String>();
		for (String[] s2 : filteredArray) {
			s1.add(s2[1]);
		}

		List x = (List) colNames.stream().filter(a -> !s1.contains(a)).collect(Collectors.toList());

		if (!x.isEmpty())
			throw new DBAppException("A column does not exist");

		for (Object s : colNames) {
			int i = 0;

			while (i < filteredArray.size()) {
				if (s.equals((filteredArray.get(i)[1]))) {

					if (parseType(colNameValue.get(s), filteredArray.get(i)[2])) {

						if (MinMaxChecker(filteredArray.get(i), colNameValue.get(s))) {
							flag = true;
							break;

						} else {
							throw new DBAppException(" Illegal Argument;An input value is out of range ");
						}
					} else {
						throw new DBAppException("Illegal Arguemt;In correct Data type in one of the input values");
					}
				}
				i++;

			}
			if (!flag) {
				throw new DBAppException("A column doesn't exist");
			}

		}

	}

	static boolean parseType(Object insrtObjt, String metadataType) {
		String type = metadataType.substring(10).toLowerCase();

		switch (type) {
			case "integer":
				return insrtObjt instanceof Integer;
			case "string":
				return insrtObjt instanceof String;
			case "double":
				return insrtObjt instanceof Double;
			case "date":
				return insrtObjt instanceof Date;
			default:
				return false;

		}

	}

	static Object returnType(String insrtObjt, String metadataType) throws DBAppException{
		String type = metadataType.substring(10).toLowerCase();

		switch (type) {
			case "integer":
				try {
					return Integer.parseInt(insrtObjt);
				}
				catch(Exception e){
					throw new DBAppException("Incorrect Type of ClusterKeyValue has been entered");
				}
			case "string":
				return insrtObjt;
			case "double":
				try{
					return Double.parseDouble(insrtObjt);
				}
				catch(Exception e) {
					throw new DBAppException("Incorrect Type of ClusterKeyValue has been entered");
				}
			case "date":
				try{
					int year = Integer.parseInt(insrtObjt.trim().substring(0, 4));
					int month = Integer.parseInt(insrtObjt.trim().substring(5, 7));
					int day = Integer.parseInt(insrtObjt.trim().substring(8));
					return new Date(year - 1900, month - 1, day);
				}
				catch(Exception e) {
					throw new DBAppException("Incorrect Type of ClusterKeyValue has been entered");
				}
			default:
				return false;

		}

	}

	static Date returnDate(String dateString) {
		int year = Integer.parseInt(dateString.trim().substring(0, 4));
		int month = Integer.parseInt(dateString.trim().substring(5, 7));
		int day = Integer.parseInt(dateString.trim().substring(8));

		Date date = new Date(year - 1900, month - 1, day);

		return date;
	}

	public static boolean MinMaxChecker(String[] filteredArray, Object o) {

		switch (filteredArray[2].substring(10).toLowerCase()) {
			case "integer":
				// ("int");
				return Integer.parseInt(filteredArray[5]) <= (int) o && Integer.parseInt(filteredArray[6]) >= (int) o;
			case "string":
				// ("string");
				return filteredArray[5].compareTo((String) o) <= 0 && filteredArray[6].compareTo((String) o) >= 0;

			case "double":
				// ("double");
				return Double.parseDouble(filteredArray[5]) <= (double) o
						&& Double.parseDouble(filteredArray[6]) >= (double) o;
			case "date":
				// ("date");
				return returnDate(filteredArray[5]).compareTo((Date) o) <= 0
						&& returnDate(filteredArray[6]).compareTo((Date) o) >= 0;

			default:
				return false;
		}

	}

	void firstPageRoutine(Hashtable<String, Object> colNameValue, Table table, String tableName) {
		Vector serPage = new Vector(); // new Page
		serPage.add(colNameValue);
		Page newPage = new Page(colNameValue.get(table.pk), colNameValue.get(table.pk), "1");
		table.noPages = false;
		table.pages.add(newPage);
		table.maxPageId = "1";


		serialize(serPage, globalPath + localPath + tableName + "\\1.class");
		serialize(table, globalPath + localPath + tableName + "\\Table.class");

	}

	public static String insert(Object primaryKey, Vector<Page> v) {
		int l = 0, r = v.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (primaryKey instanceof Date) {
				Date DateKey = (Date) primaryKey;

				if (DateKey.compareTo((Date) v.get(m).min) >= 0 && DateKey.compareTo((Date) v.get(m).max) <= 0) {
					return v.get(m).getId();
				}

//				if (primaryKey >= v.get(m).min && primaryKey <= v.get(m).max) {
//					return m;
//				}

				if (DateKey.compareTo((Date) v.get(m).max) > 0)
					l = m + 1;

//				if (primaryKey > v.get(m).max)
//					l = m + 1;

					// If x is smaller, ignore right half
				else
					r = m - 1;
			}

			else if (primaryKey instanceof String) {
				String StringKey = (String) primaryKey;

				if (StringKey.compareTo((String) v.get(m).min) >= 0
						&& StringKey.compareTo((String) v.get(m).max) <= 0) {
					return v.get(m).getId();
				}

//				if (primaryKey >= v.get(m).min && primaryKey <= v.get(m).max) {
//					return m;
//				}

				if (StringKey.compareTo((String) v.get(m).max) > 0)
					l = m + 1;

//				if (primaryKey > v.get(m).max)
//					l = m + 1;

					// If x is smaller, ignore right half
				else
					r = m - 1;
			}

			else if (primaryKey instanceof Double) {
				Double IntKey = (Double) primaryKey;
				if (IntKey >= (Double) v.get(m).min && IntKey <= (Double) v.get(m).max) {
					return v.get(m).getId();
				}

				if (IntKey > (Double) v.get(m).max)
					l = m + 1;

					// If x is smaller, ignore right half
				else
					r = m - 1;
			} else {
				Integer IntKey = (int) primaryKey;
				if (IntKey >= (int) v.get(m).min && IntKey <= (int) v.get(m).max) {
					return v.get(m).getId();
				}

				if (IntKey > (int) v.get(m).max)
					l = m + 1;

					// If x is smaller, ignore right half
				else
					r = m - 1;
			}
		}
		return "-1";

		// if we reach here, then element was
		// not present

	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {


		ArrayList<String[]> tableNames = readCSV(globalPath + "\\src\\main\\resources\\tableNames.csv");

		ArrayList<String[]> metaDataArray = readCSV(globalPath + "\\src\\main\\resources\\metadata.csv");

		ArrayList<String[]> filteredArray = (ArrayList<String[]>) metaDataArray.stream()
				.filter(a -> a[0].equals(tableName)).collect(Collectors.toList());

		boolean tableNotExist = true;
		for (String[] s : tableNames)
			for (String s1 : s) {
				if (s1.equals(tableName)) {
					tableNotExist = false;
				}
			}

		if (tableNotExist) {
			throw new DBAppException("Table does not exist");
		}
		Set colNames = columnNameValue.keySet();
		inputChecker(colNames, filteredArray, columnNameValue);

		String tablePath = globalPath + localPath + tableName + "\\Table.class";

		Table table = (Table) deSerialize(tablePath);

		if (table.noPages)
			throw new DBAppException("Cannot Update Empty Table");

		if(columnNameValue.keySet().contains(table.pk))
			throw new DBAppException("Cannot Update Primary Key of Table");

		String type = "";
		for (String[] s1 : filteredArray) {
			if (s1[1].equals(table.pk))
				type = s1[2];
		}

		Object primValue = null;
			primValue = returnType(clusteringKeyValue, type);


		String pageId = insert(primValue, table.pages);
		if (pageId.equals("-1"))
			throw new DBAppException("Tuple Doesn't Exist");
		///////////////////////////////////////////// Add throw excpetion to method
		///////////////////////////////////////////// ////////////////

		Vector page = ((Vector) deSerialize(globalPath + localPath + tableName + "\\" + pageId + ".class"));

		int tupleIndex = binSearch(table.pk, primValue, page);

		if (tupleIndex == -1)
			throw new DBAppException("Tuple Doesn't Exist");

		Hashtable<String, Object> tuple = (Hashtable<String, Object>) page.get(tupleIndex);
		for (Object key : colNames) {
			tuple.put((String) key, columnNameValue.get(key));
		}

		serialize(page, globalPath + localPath + tableName + "\\" + pageId + ".class");
		serialize(table, globalPath + localPath + tableName + "\\" + "Table" + ".class");
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		ArrayList<String[]> tableNames = readCSV(globalPath + "\\src\\main\\resources\\tableNames.csv");

		ArrayList<String[]> metaDataArray = readCSV(globalPath + "\\src\\main\\resources\\metadata.csv");

		ArrayList<String[]> filteredArray = (ArrayList<String[]>) metaDataArray.stream()
				.filter(a -> a[0].equals(tableName)).collect(Collectors.toList());

		boolean tableExists = true;
		for (String[] s : tableNames)
			for (String s1 : s) {
				if (s1.equals(tableName)) {
					tableExists = false;
				}
			}

		if (tableExists) {
			throw new DBAppException("Table does not exist");
		}
		Set colNames = columnNameValue.keySet();
		inputChecker(colNames, filteredArray, columnNameValue);

		String tablePath = globalPath + localPath + tableName + "\\Table.class";

		Table table = (Table) deSerialize(tablePath);

		if (table.noPages)
			throw new DBAppException("Cannot Delete from empty table");
		String pk = table.pk;
		Object primKey = columnNameValue.get(pk);
		if (!colNames.contains(pk))
			noClusterKey(tableName, table, columnNameValue);
		else {

			String pageId = insert(primKey, table.pages);
			Vector page = ((Vector) deSerialize(globalPath + localPath + tableName + "\\" + pageId + ".class"));
			int index = binSearch(pk, primKey, page); // badal success khaletha index 3shan binSearch btraga3 el index
			if (index == -1) // i didn't find the element badal success khaletha index bardo ghayart el esm
				// bas
				throw new DBAppException(" Row doesn't exist ");
			else {
				Hashtable row = (Hashtable) page.get(index);
				for (Object s : colNames) {
					if (compare(row.get(s), columnNameValue.get(s)) != 0) { // hena b check law kol value fel row heya
						// heya el value el fel hashtable el
						// dakhlaly
						throw new DBAppException("row doesn't exist");
					}
				}
				page.remove(index); // hena h remove el tuple aw el row
			}

			Page p = null;
			for (Page p1 : table.pages) {
				if (p1.id.equals(pageId))
					p = p1;
			}

			if (page.isEmpty()) {
				File f = new File(globalPath + localPath + tableName + "\\" + pageId + ".class");
				f.delete();

				table.pages.remove(p);

				if (table.pages.isEmpty())
					table.noPages = true;
			}

			else {
				p.min = ((Hashtable<String, Object>) (page.get(0))).get(pk);
				p.max = ((Hashtable<String, Object>) (page.get(page.size() - 1))).get(pk);
				serialize(page, globalPath + localPath + tableName + "\\" + pageId + ".class");
				serialize(table, globalPath + localPath + tableName + "\\" + "Table" + ".class");

			}
		}
	}

	@SuppressWarnings("unchecked")
	void noClusterKey(String tableName, Table t, Hashtable<String, Object> columnNameValue) {

		Vector<Page> pages = t.pages;

		for (int j =  0 ; j<pages.size() ;) {
			Page p = pages.get(j);
			String id = p.id;
			Vector<Hashtable<String, Object>> page = ((Vector) deSerialize(
					globalPath + localPath + tableName + "\\" + id + ".class"));

			for (int i =  0 ; i<page.size();) {
				Hashtable<String, Object> row = page.get(i) ;
				Set<String> cols = columnNameValue.keySet();
				boolean flag = true;
				for (String s : cols) {
					if (compare(columnNameValue.get(s), row.get(s)) != 0) {
						flag = false;
						i++;
					}
				}

				if (flag == true) {
					page.remove(row);
//					("1");
				}
			}
			if (page.isEmpty()) {
				File f = new File(globalPath + localPath + tableName + "\\" + id + ".class");
				f.delete();

				t.pages.remove(p);
				if (t.pages.isEmpty()) {
					t.noPages = true;
					t.maxPageId = "";
				}
				serialize(t, globalPath + localPath + tableName + "\\" + "Table" + ".class");

			} else {

				p.min = ((Hashtable<String, Object>) (page.get(0))).get(t.pk);
				p.max = ((Hashtable<String, Object>) (page.get(page.size() - 1))).get(t.pk);
				serialize(page, globalPath + localPath + tableName + "\\" + id + ".class");
				serialize(t, globalPath + localPath + tableName + "\\" + "Table" + ".class");
				j++;
			}

		}

	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		DBApp a = new DBApp();
//
//
//
//		 ArrayList<String[]> metadata = readCSV("src/main/resources/metadata.csv");
//		ArrayList<String[]> tableNames = readCSV("src/main/resources/tableNames.csv");
//
////		ArrayList<String[]> filteredArray = (ArrayList<String[]>) s.stream()
////				.filter(b -> b[0].equals("students")).collect(Collectors.toList());
//
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








//		a.createTable(strTableName, cluster, htblColNameType, htblColNameMin,
//					htblColNameMax);

//			a.deleteFromTable("height2",htblColNameValue);
//			a.updateTable( strTableName , "29" , htblColNameValue );
////         finally {
////			Table table = (Table) a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "height2" + "\\Table.class");
////			System.out.println(table);
////			for( Page p : table.pages )
////			{
////				System.out.println( "Page Id : "  + p.id + " " );
////				Vector Tp= (Vector)	a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "height2" + "\\" + p.id + ".class");
////				for(int i=0;i<Tp.size();i++){
////					System.out.print(  (((Hashtable<String,Object>)Tp.get(i)) + "  ----  ") );
////				}
////			}
////		}
//
////		Vector Tp= (Vector)	a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "height" + "\\1.class");
////		for(int i=0;i<Tp.size();i++){
////			System.out.print(  (((Hashtable<String,Object>)Tp.get(i)) + " ") );
////		}
////
////		System.out.println();
////			Vector p2= (Vector)	a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "height" + "\\2.class");
////			for(int i=0;i<p2.size();i++){
////				System.out.print(  (((Hashtable<String,Object>)p2.get(i)) + " ") );
////			}
//
////			Table table = (Table) a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "height" + "\\Table.class");
////			System.out.println(table);
////
////			for( Page p : table.pages )
////			{
////				System.out.println(p.id);
////			}
////		Table table = (Table) a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "height" + "\\Table.class");
////		System.out.println(table);
//
//
////
////		Hashtable htbl = new Hashtable();
////		htbl.put("gpa",0.1);
////		//htbl.put("id",81);
//
//
//
//
////
////		Table tStudents = (Table) a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "students" + "\\Table.class");
////		Vector Tp= (Vector)	a.deSerialize(System.getProperty("user.dir") + "\\src\\main\\resources\\data\\" + "students" + "\\1.class");
////        for(int i=0;i<Tp.size();i++){
////         	(Tp.get(i).toString());
////		 }
////		finally {
////			Vector<Hashtable<String, Object>> b = (Vector) a.deSerialize(
////					"C:\\Users\\Mohamed Amr\\Desktop\\Desktop\\GUC\\Semester 6\\DB II\\Project\\GUC_437_53_5863_2021-04-11T23_56_39\\DB2Project\\src\\main\\resources\\data\\Student30\\1.class");
////			for (Hashtable<String, Object> t : b) {
////
////				System.out.print(t.get("id") + " ");
////
////			}
////		}
//
////		Vector<Hashtable<String, Object>> b = (Vector) a.deSerialize(
////				"C:\\Users\\Mohamed Amr\\Desktop\\Desktop\\GUC\\Semester 6\\DB II\\Project\\GUC_437_53_5863_2021-04-11T23_56_39\\DB2Project\\src\\main\\resources\\data\\Student1\\1.class");
////		Vector<Hashtable<String, Object>> c = (Vector) a.deSerialize(
////				"C:\\Users\\Mohamed Amr\\Desktop\\Desktop\\GUC\\Semester 6\\DB II\\Project\\GUC_437_53_5863_2021-04-11T23_56_39\\DB2Project\\src\\main\\resources\\data\\Student1\\2.class");
////		Table table = (Table) a.deSerialize(
////				"C:\\Users\\Mohamed Amr\\Desktop\\Desktop\\GUC\\Semester 6\\DB II\\Project\\GUC_437_53_5863_2021-04-11T23_56_39\\DB2Project\\src\\main\\resources\\data\\Student1\\Table.class");
////		for (Hashtable<String, Object> t : b) {
////
////			System.out.print(t.get("id") + " ");
////
////		}
////
////
//
//		ArrayList<String[]> metaDataArray = readCSV(a.globalPath + "\\src\\main\\resources\\metadata.csv");
//
//
//
//
////		try {
////			a.inputChecker( htblColNameValue5.keySet() , filteredArray , htblColNameValue5 );
////		} catch (DBAppException e) {
////			e.printStackTrace();
////		}
//





	}
	//}
	public int binSearch(String pk, Object primaryKey, Vector<Hashtable<String, Object>> v) {
		int l = 0, r = v.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			Hashtable<String, Object> currRow = v.get(m);

			if (compare(currRow.get(pk), primaryKey) == 0) {
				return m;
			}

			else if (compare(primaryKey, currRow.get(pk)) > 0)
				l = m + 1;

			else
				r = m - 1;

		}
		return -1;

		// if we reach here, then element was
		// not present

	}

	@SuppressWarnings("finally")
	public static ArrayList<String[]> readCSV(String path) {
		// "C:\\Users\\Mohamed Amr\\Desktop\\Desktop\\GUC\\Semester 6\\DB
		// II\\Project\\GUC_437_53_5863_2021-04-11T23_56_39\\DB2Project\\src\\main\\resources\\metadata.csv"
		BufferedReader csvReader;
		ArrayList<String[]> res = new ArrayList<String[]>();
		try {
			csvReader = new BufferedReader(new FileReader(path));
			String row = null;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				res.add(data);
			}
			csvReader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			return res;
		}

	}

	void writeCSV(String path, List<List<String>> row ) {
		FileWriter csvWriter;
		try {
			csvWriter = new FileWriter(globalPath + path, true);

			for (List<String> rowData : row) {
				csvWriter.append(String.join(",", rowData));
				csvWriter.append("\n");
			}

			csvWriter.flush();
			csvWriter.close();

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

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

}
