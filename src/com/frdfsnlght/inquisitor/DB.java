/*
 * Copyright 2012 frdfsnlght <frdfsnlght@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.frdfsnlght.inquisitor;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.ConnectionEventListener;
import javax.sql.rowset.serial.SerialClob;

/**
 * 
 * @author frdfsnlght <frdfsnlght@gmail.com>
 */
public final class DB {

	private static final Set<String> OPTIONS = new HashSet<String>();
	private static final Set<String> RESTART_OPTIONS = new HashSet<String>();
	private static final Options options;

	private static final Set<DBListener> listeners = new HashSet<DBListener>();
	
	
	public static enum ConnectionType {
		BACKGROUND,
		FOREGROUND
	}

	static {
		OPTIONS.add("debug");
		OPTIONS.add("url");
		OPTIONS.add("username");
		OPTIONS.add("password");
		OPTIONS.add("prefix");
		OPTIONS.add("shared");

		RESTART_OPTIONS.add("url");
		RESTART_OPTIONS.add("username");
		RESTART_OPTIONS.add("password");
		RESTART_OPTIONS.add("shared");

		options = new Options(DB.class, OPTIONS, "inq.db",
				new OptionsListener() {
					@Override
					public void onOptionSet(Context ctx, String name,
							String value) {
						ctx.sendLog("database option '%s' set to '%s'", name,
								value);
						if (RESTART_OPTIONS.contains(name)) {
							Config.save(ctx);
							stop();
							start();
						}
					}

					@Override
					public String getOptionPermission(Context ctx, String name) {
						return name;
					}
				});
	}

	private static HashMap<ConnectionType, Connection> connectionList = new HashMap<ConnectionType, Connection>();
	private static boolean didUpdates = false;

	public static void init() {
	}

	public static void addListener(DBListener listener) {
		listeners.add(listener);
	}
	

	public static void start()
	{
		int openCount = 0;
		for(Connection c : connectionList.values())
		{
			try {
				if( c != null && !c.isClosed() )
				{
					openCount++;
					continue;
				}
			} catch (SQLException se) { }
			
			break;
		}
		
		if( openCount >= ConnectionType.values().length ) return;
//			if ( dbFor  != null && !dbFor.isClosed() &&
//				 dbBack != null && !dbBack.isClosed()  )
//				return;

		try {
			if (getUrl() == null)
				throw new InquisitorException("url is not set");
			if (getUsername() == null)
				throw new InquisitorException("username is not set");
			if (getPassword() == null)
				throw new InquisitorException("password is not set");
			
			connect(ConnectionType.BACKGROUND);
			connect(ConnectionType.FOREGROUND);

		} catch (Exception e) {
			Utils.warning("database connection cannot be completed: %s",
					e.getMessage());
		}
	}

	public static void stop() {
		for(ConnectionType type: connectionList.keySet())
		{
			Connection db = connectionList.get(type);

			try {
				if( db != null )
				{
					if(type == ConnectionType.BACKGROUND)
					{
						for (DBListener listener : listeners)
							listener.onDBDisconnecting();
					}
					
					db.close();
				}
			} catch (SQLException se) {
			} finally {
				connectionList.put(type, null);
			}
			
			
		}

		Utils.info("disconnected from database");
	}

	/* Begin options */

	public static boolean getDebug() {
		return Config.getBooleanDirect("db.debug", false);
	}

	public static void setDebug(boolean b) {
		Config.setPropertyDirect("db.debug", b);
	}

	public static String getUrl() {
		return Config.getStringDirect("db.url", null);
	}

	public static void setUrl(String s) {
		if ((s != null) && (s.equals("-") || s.equals("*")))
			s = null;
		Config.setPropertyDirect("db.url", s);
	}

	public static String getUsername() {
		return Config.getStringDirect("db.username", null);
	}

	public static void setUsername(String s) {
		if ((s != null) && (s.equals("-") || s.equals("*")))
			s = null;
		Config.setPropertyDirect("db.username", s);
	}

	public static String getPassword() {
		if (getRealPassword() == null)
			return null;
		return "*******";
	}

	public static String getRealPassword() {
		return Config.getStringDirect("db.password", null);
	}

	public static void setPassword(String s) {
		if ((s != null) && (s.equals("-") || s.equals("*")))
			s = null;
		Config.setPropertyDirect("db.password", s);
	}

	public static String getPrefix() {
		return Config.getStringDirect("db.prefix", null);
	}

	public static void setPrefix(String s) {
		if ((s != null) && (s.equals("-") || s.equals("*")))
			s = null;
		if (s != null) {
			if (!s.matches("^\\w+$"))
				throw new IllegalArgumentException("illegal character");
		}
		Config.setPropertyDirect("db.prefix", s);
	}

	public static boolean getShared() {
		return Config.getBooleanDirect("db.shared", true);
	}

	public static void setShared(boolean b) {
		Config.setPropertyDirect("db.shared", b);
	}

	public static void getOptions(Context ctx, String name)
			throws OptionsException, PermissionsException {
		options.getOptions(ctx, name);
	}

	public static String getOption(Context ctx, String name)
			throws OptionsException, PermissionsException {
		return options.getOption(ctx, name);
	}

	public static void setOption(Context ctx, String name, String value)
			throws OptionsException, PermissionsException {
		options.setOption(ctx, name, value);
	}

	/* End options */

	public static String tableName(String baseName) {
		String prefix = getPrefix();
		if (prefix != null)
			baseName = prefix + baseName;
		return '`' + baseName + '`';
	}

	public static Connection connect(ConnectionType type) throws SQLException {
		Connection db = null;
		if (!isConnected(type)) {
			db = connectionList.get(type);
			if (db != null) {
				Utils.warning("unexpectedly disconnected from database");
//				connectionList.put(type, null);
			}
			connectionList.put(type,
					db = DriverManager.getConnection(getUrl(), getUsername(),
					getRealPassword()) );
			
			db.setAutoCommit(true);
			db.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			Utils.info("connected to database");
			if (!didUpdates)
				doUpdates(type);
			
			if( type == ConnectionType.BACKGROUND )
			{
				for (DBListener listener : listeners)
					listener.onDBConnected();
			}
		}
		return db != null ? db : connectionList.get(type);
	}

	public static boolean isConnected(ConnectionType type) {
		try {
			Connection db = connectionList.get(type);
			return (db != null) && (!db.isClosed()) && db.isValid(10);
		} catch (SQLException se) {
			return false;
		}
	}

	public static PreparedStatement prepare(String sql) throws SQLException {
		return prepare(sql, ConnectionType.BACKGROUND);
	}
	public static PreparedStatement prepare(String sql, ConnectionType type) throws SQLException {
		if (getDebug())
			Utils.debug(sql);
		return connect(type).prepareStatement(sql);
	}

	public static Clob encodeToJSON(Object obj) throws SQLException {
		if (obj == null)
			return null;
		return new SerialClob(JSON.encode(obj).toCharArray());
	}

	public static Object decodeFromJSON(Clob clob) throws SQLException {
		if (clob == null)
			return null;
		return JSON.decode(clob.getSubString(1L, (int) clob.length()));
	}

	public static Date decodeTimestamp(Timestamp ts) throws SQLException {
		if (ts == null)
			return null;
		return new Date(ts.getTime());
	}

	public static Timestamp encodeTimestamp(Date d) throws SQLException {
		if (d == null)
			return null;
		return new Timestamp(d.getTime());
	}

	public static boolean tableExists(String name, ConnectionType type) throws SQLException {
		PreparedStatement stmt = prepare("SHOW TABLES LIKE ?", type);
		String prefix = getPrefix();
		if (prefix != null)
			name = prefix + name;
		stmt.setString(1, name);
		ResultSet rs = stmt.executeQuery();
		boolean exists = rs.next();
		rs.close();
		stmt.close();
		return exists;
	}

	public static boolean dropTable(String name, ConnectionType type) throws SQLException {
		if (!tableExists(name, type))
			return false;
		Utils.debug("dropping table '%s'", name);
		PreparedStatement stmt = prepare("DROP TABLE " + tableName(name), type);
		stmt.executeUpdate();
		stmt.close();
		return true;
	}

	public static boolean columnExists(String tableName, String columnName, ConnectionType type)
			throws SQLException {
		PreparedStatement stmt = prepare("SHOW COLUMNS FROM "
				+ tableName(tableName) + " LIKE ?", type);
		stmt.setString(1, columnName);
		ResultSet rs = stmt.executeQuery();
		boolean exists = rs.next();
		rs.close();
		stmt.close();
		return exists;
	}

	public static boolean addColumn(String tableName, String columnName,
			String columnDef, ConnectionType type) throws SQLException {
		if (columnExists(tableName, columnName, type))
			return false;
		Utils.debug("adding column '%s' to table '%s'", columnName, tableName);
		PreparedStatement stmt = prepare("ALTER TABLE " + tableName(tableName)
				+ " ADD `" + columnName + "` " + columnDef, type);
		stmt.executeUpdate();
		stmt.close();
		return true;
	}

	public static boolean dropColumn(String tableName, String columnName, ConnectionType type)
			throws SQLException {
		if (!columnExists(tableName, columnName, type))
			return false;
		Utils.debug("dropping column '%s' from table '%s'", columnName,
				tableName);
		PreparedStatement stmt = prepare("ALTER TABLE " + tableName(tableName)
				+ " DROP `" + columnName + "`", type);
		stmt.executeUpdate();
		stmt.close();
		return true;
	}

	private static void doUpdates(ConnectionType type) throws SQLException {
		PreparedStatement stmt1 = null;
		PreparedStatement stmt2 = null;
		ResultSet rs = null;

		String[][] updates = new String[][] {
				new String[] { "blocksBroken", "totalBlocksBroken", "int" },
				new String[] { "blocksPlaced", "totalBlocksPlaced", "int" },
				new String[] { "itemsDropped", "totalItemsDropped", "int" },
				new String[] { "itemsPickedUp", "totalItemsPickedUp", "int" },
				new String[] { "itemsCrafted", "totalItemsCrafted", "int" },
				new String[] { "travelDistances", "totalDistanceTraveled",
						"float" } };

		Utils.debug("doing DB updates");

		try {
			dropTable("versions", type);
			if (tableExists("players", type)) {
				for (int i = 0; i < updates.length; i++) {
					String[] data = updates[i];
					if (addColumn("players", data[1], data[2] + " DEFAULT 0", type)) {
						stmt1 = prepare("SELECT id," + data[0] + " FROM "
								+ tableName("players"), type);
						rs = stmt1.executeQuery();
						stmt2 = prepare("UPDATE " + tableName("players")
								+ " SET " + data[1] + "=? WHERE id=?", type);
						while (rs.next()) {
							int id = rs.getInt("id");
							TypeMap map = (TypeMap) decodeFromJSON(rs
									.getClob(data[0]));
							if (data[2].equals("int")) {
								int count = totalIntegerTypeMap(map);
								stmt2.setInt(1, count);
							} else {
								float count = totalFloatTypeMap(map);
								stmt2.setFloat(1, count);
							}
							stmt2.setInt(2, id);
							stmt2.executeUpdate();
						}
						stmt2.close();
						stmt2 = null;
						rs.close();
						stmt1.close();
					}
				}
			}

			if (tableExists("players", type)) {
				if (columnExists("players", "blocksBroken", type)) {
					StringBuilder sb = new StringBuilder();
					sb.append("`id`");
					for (Statistic stat : PlayerStats.group.getStatistics()) {
						if (!stat.isMapped())
							continue;
						sb.append(",`").append(stat.getName()).append('`');
					}
					stmt1 = prepare("SELECT " + sb.toString() + " FROM "
							+ tableName("players"), type);
					rs = stmt1.executeQuery();
					Map<Integer, Object> data = new HashMap<Integer, Object>();
					while (rs.next()) {
						int id = rs.getInt("id");
						TypeMap mappedObjects = new TypeMap();
						for (Statistic stat : PlayerStats.group.getStatistics()) {
							if (!stat.isMapped())
								continue;
							TypeMap map = (TypeMap) decodeFromJSON(rs
									.getClob(stat.getName()));
							mappedObjects.put(stat.getName(), map);
						}
						data.put(id, mappedObjects);
					}
					rs.close();
					rs = null;
					stmt1.close();
					stmt1 = null;

					for (Statistic stat : PlayerStats.group.getStatistics()) {
						if (!stat.isMapped())
							continue;
						dropColumn("players", stat.getName(), type);
					}

					if (!columnExists("players", Statistic.MappedObjectsColumn, type))
						addColumn("players", Statistic.MappedObjectsColumn,
								Statistic.Type.OBJECT.getSQLDef(), type);

					stmt2 = prepare("UPDATE " + tableName("players") + " SET `"
							+ Statistic.MappedObjectsColumn
							+ "`=? WHERE `id`=?", type);
					if (getDebug())
						Utils.debug("Updating %s players...", data.keySet()
								.size());
					for (int id : data.keySet()) {
						stmt2.setClob(1, encodeToJSON((TypeMap) data.get(id)));
						stmt2.setInt(2, id);
						if (getDebug())
							Utils.debug("updating player %s", id);
						stmt2.executeUpdate();
					}
					stmt2.close();
					stmt2 = null;
				}
			}

			if ((!Config.getBooleanDirect("db.v2-14-fix", false))
					&& tableExists("players", type)
					&& columnExists("players", Statistic.MappedObjectsColumn, type)) {

				Utils.info("Applying v2.14 DB fix to existing players...");

				stmt1 = prepare("SELECT `id`,`" + Statistic.MappedObjectsColumn
						+ "` FROM " + tableName("players"), type);
				rs = stmt1.executeQuery();

				stmt2 = prepare("UPDATE " + tableName("players") + " SET `"
						+ Statistic.MappedObjectsColumn + "`=? WHERE `id`=?", type);

				while (rs.next()) {
					int id = rs.getInt("id");
					TypeMap mapped = (TypeMap) DB.decodeFromJSON(rs
							.getClob(Statistic.MappedObjectsColumn));
					boolean needUpdate = false;
					for (String key : mapped.keySet()) {
						Object value = mapped.get(key);
						if (value instanceof Long) {
							needUpdate = true;
							mapped.put(key, null);
						}
					}
					if (needUpdate) {
						Utils.info("Reparing player %s", id);
						stmt2.setClob(1, encodeToJSON(mapped));
						stmt2.setInt(2, id);
						stmt2.executeUpdate();
					}
				}
				rs.close();
				rs = null;
				stmt1.close();
				stmt1 = null;
				stmt2.close();
				stmt2 = null;

				Config.setPropertyDirect("db.v2-14-fix", true);
			}

			if (tableExists("players", type) && !columnExists("players", "uuid", type)) {
				addColumn("players", "uuid", "varchar(36)", type);
			}

			didUpdates = true;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt1 != null)
					stmt1.close();
				if (stmt2 != null)
					stmt2.close();
			} catch (SQLException se) {
			}
		}
	}

	private static int totalIntegerTypeMap(TypeMap m) {
		if (m == null)
			return 0;
		int t = 0;
		for (String key : m.getKeys())
			t += m.getInt(key);
		return t;
	}

	private static float totalFloatTypeMap(TypeMap m) {
		if (m == null)
			return 0;
		float t = 0;
		for (String key : m.getKeys())
			t += m.getFloat(key);
		return t;
	}

	public static interface DBListener {
		public void onDBConnected();

		public void onDBDisconnecting();
	}

}
