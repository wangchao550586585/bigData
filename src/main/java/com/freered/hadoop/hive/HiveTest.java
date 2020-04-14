package com.freered.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveTest {
	public static void main(String[] args) throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://hadoop01:10000/default", "root", "root");
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("select count(*) from t_emp");
		if (rs.next()) {
			System.out.println(rs.getInt(1));
		}
		rs.close();
		st.close();
		con.close();
	}
}
