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
        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");
        System.out.println("Opened database successfully.");
        
        Statement stmt = conn.createStatement();  
        ResultSet T_delta = null;   //to count the size of delta table

        // check if there are such tables in the given database.
        stmt.executeUpdate("DROP TABLE IF EXISTS tracking;");
        stmt.executeUpdate("DROP TABLE IF EXISTS delta;");
        stmt.executeUpdate("DROP TABLE IF EXISTS T;");
        stmt.executeUpdate("DROP TABLE IF EXISTS oldT;");
        stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");

        // a table to collect paths 
        stmt.executeUpdate("CREATE TABLE tracking(airline char(32), origin char(32), destination char(32), stops int);");
        stmt.executeUpdate("INSERT INTO tracking('airline','origin','destination', Stops) SELECT airline, origin, destination,"+ 0 + " FROM Flight;");
        
        // a table for Δ
        stmt.executeUpdate("CREATE TABLE delta(airline char(32), origin char(32), destination char(32));");
        stmt.executeUpdate("INSERT INTO delta('airline', 'origin', 'destination') SELECT airline, origin, destination from Flight;");

        // a table for T
        stmt.executeUpdate("CREATE TABLE T(airline char(32), origin char(32), destination char(32));");
        stmt.executeUpdate("INSERT INTO T(airline, origin, destination) SELECT airline, origin, destination FROM Flight;");

        // a table for T_old
        stmt.executeUpdate("CREATE TABLE oldT(airline char(32), origin char(32), destination char(32));");
       
        T_delta = stmt.executeQuery("SELECT COUNT(*) FROM delta;");
        int T_size = T_delta.getInt(1);
        int N_stops = 1;
        //System.out.println(T_size);

        while(T_size > 0) {
            //T_old := T
            stmt.executeUpdate("DELETE FROM oldT;");
            stmt.executeUpdate("INSERT INTO oldT(airline, origin, destination) SELECT airline, origin, destination FROM T;"); 
           
            //old = null;
            //old = stmt.executeQuery("SELECT COUNT(*) FROM oldT;");
            //old_s = old.getInt(1);
            //System.out.println(old_s + "oldT");

            //T := (SELECT * FROM T)
            //  UNION
            //  (SELECT x.A, y.B FROM G x, Δ y           
            //  WHERE x.B = y.A)
            stmt.executeUpdate("INSERT INTO T(airline, origin, destination) SELECT f.airline, f.origin, d.destination FROM Flight f, delta d WHERE f.destination = d.origin AND f.airline = d.airline AND f.origin <> d.destination;");

            //curr= null;
            //curr = stmt.executeQuery("SELECT COUNT(*) FROM T;");
            //curr_s = curr.getInt(1);
            //System.out.println(curr_s + "newT");

            //Δ := T - T_old
            stmt.executeUpdate("DELETE FROM delta;");
            stmt.executeUpdate("INSERT INTO delta(airline, origin, destination) SELECT airline, origin, destination FROM T EXCEPT SELECT airline, origin, destination FROM oldT;");
            
            stmt.executeUpdate("INSERT INTO tracking(airline, origin, destination, Stops) SELECT airline, origin, destination," + N_stops + " FROM delta;");
            N_stops++;

            T_delta = stmt.executeQuery("SELECT COUNT(*) FROM delta;");
            T_size = T_delta.getInt(1);
            //System.out.println(T_size + "delta");
        }
        stmt.executeUpdate("DROP TABLE IF EXISTS delta;");
        stmt.executeUpdate("DROP TABLE IF EXISTS T;");
        stmt.executeUpdate("DROP TABLE IF EXISTS oldT;");

        // store the collected data to the output table, Connected
        stmt.executeUpdate("CREATE TABLE Connected(airline char(32), origin char(32), destination char(32), Stops int);");
        stmt.executeUpdate("INSERT INTO Connected(airline, origin, destination, stops) SELECT airline, origin, destination, MIN(stops) FROM tracking GROUP BY airline, origin, destination;");

        stmt.executeUpdate("DROP TABLE IF EXISTS tracking;");

        /* Provided code */
        // Close the ResultSets and Statement objects.
        T_delta.close();
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