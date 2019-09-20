/**
* This Java program exemplifies the basic usage of JDBC.
* Requirements:
* (1) JDK 8.0+
* (2) SQLite3.
* (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlitejdbc/downloads/sqlite-jdbc-3.8.7.jar).
*/
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
public class PA2 {
 public static void main(String[] args) {
    Connection conn = null; // Database connection.
    try {
        // Load the JDBC class.
        Class.forName("org.sqlite.JDBC");
        // Get the connection to the database.
        // - "jdbc" : JDBC connection name prefix.
        // - "sqlite" : The concrete database implementation
        // (e.g., sqlserver, postgresql).
        // - "pa2.db" : The name of the database. In this project,
        // we use a local database named "pa2.db". This can also
        // be a remote database name.
        conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");
        System.out.println("Opened database successfully.");
        // Use case #1: Create and populate a table.
        // Get a Statement object.
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS Student;");
         // Student table is being created just as an example. You
         // do not need Student table in PA2
        stmt.executeUpdate("Flight (airline, origin, destination);");
        stmt.executeUpdate("INSERT INTO Flight VALUES('Delta','Aberdeen','London Gatwick'),('Delta','Aberdeen','London Heathrow');");
 
        // Use case #2: Query the Student table with Statement.
        // Returned query results are stored in a ResultSet
        // object.
        ResultSet rset = stmt.executeQuery("SELECT * from Flight;");
        // Print the FirstName and LastName columns.
        System.out.println ("\nStatement result:");
        // This shows how to traverse the ResultSet object.
        while (rset.next()) {
            // Get the attribute value.
            System.out.print(rset.getString("airline"));
            System.out.print("---");
            System.out.println(rset.getString("origin"));
            System.out.print("---");
            System.out.println(rset.getString("destination"));
        }

        // Use case #3: Query the Student table with
        // PreparedStatement (having wildcards).
         PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM Flight WHERE airline = Delta;");
        // Assign actual value to the wildcard.
        pstmt.setString (1, "F1");
        rset = pstmt.executeQuery ();
        System.out.println ("\nPrepared statement result:");
        while (rset.next()) {
            System.out.print(rset.getString("airline"));
            System.out.print("---");
            System.out.println(rset.getString("origin"));
            System.out.print("---");
            System.out.println(rset.getString("destination"));
        }
        // Close the ResultSet and Statement objects.
        rset.close();
        stmt.close();
    } catch (Exception e) {
        throw new RuntimeException("There was a runtime problem!", e);
    } finally {
        try {
            if (conn != null) conn.close();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot close the connection!", e);
        }
    }
}
}
