/*
 * TPC-C Delivery transaction test
 */

// JdbcRunner settings -----------------------------------------------

// Oracle Database
// var jdbcUrl = "jdbc:oracle:thin://@k02c5:1521/ora11a";

// MySQL
var jdbcUrl = "jdbc:mysql://localhost:3306/jdbcrunner?rewriteBatchedStatements=true";

// PostgreSQL
// var jdbcUrl = "jdbc:postgresql://k01sl6:5432/jdbcrunner";

var jdbcDriver = "";
var jdbcUser = "jdbcrunner";
var jdbcPass = "";
var isLoad = false;
var warmupTime = 0;
var measurementTime = 60;
var nTxTypes = 1;
var nAgents = 2;
var connPoolSize = nAgents;
var stmtCacheSize = 10;
var isAutoCommit = false;
var sleepTime = 0;
var throttle = 0;
var isDebug = false;
var isTrace = false;
var logDir = "logs";

// Application settings ----------------------------------------------

var SCALE = 10;

// JdbcRunner functions ----------------------------------------------

function init() {
    if (getId() == 0) {
        // This block is performed only by Agent 0.
        
        // Oracle Database
        /*
        try {
            execute("DROP TABLE new_orders");
        } catch (e) {
            warn(e);
            rollback();
        }
        */
        
        // MySQL, PostgreSQL
        execute("DROP TABLE IF EXISTS new_orders");
        
        // Oracle Database
        /*
        execute("CREATE TABLE new_orders ("
            + "no_o_id NUMBER, "
            + "no_d_id NUMBER, "
            + "no_w_id NUMBER, "
            + "PRIMARY KEY (no_w_id, no_d_id, no_o_id))");
        */
        
        // MySQL
        execute("CREATE TABLE new_orders ("
            + "no_o_id INT, "
            + "no_d_id INT, "
            + "no_w_id INT, "
            + "PRIMARY KEY (no_w_id, no_d_id, no_o_id)) "
            + "ENGINE = InnoDB");
        
        // PostgreSQL
        /*
        execute("CREATE TABLE new_orders ("
            + "no_o_id INT, "
            + "no_d_id INT, "
            + "no_w_id INT, "
            + "PRIMARY KEY (no_w_id, no_d_id, no_o_id))");
        */
        
        var no_o_id = new Array();
        var no_d_id = new Array();
        var no_w_id = new Array();
        
        for (var w_id = 1; w_id <= SCALE; w_id++) {
            info("warehouse : " + w_id);
            
            for (var d_id = 1; d_id <= 10; d_id++) {
                for (var o_id = 2101; o_id <= 3000; o_id++) {
                    no_o_id.push(o_id);
                    no_d_id.push(d_id);
                    no_w_id.push(w_id);
                }
                
                executeBatch("INSERT INTO new_orders "
                    + "(no_o_id, no_d_id, no_w_id) "
                    + "VALUES ($int, $int, $int)",
                    no_o_id, no_d_id, no_w_id);
                
                no_o_id.length = 0;
                no_d_id.length = 0;
                no_w_id.length = 0;
                
                commit();
            }
        }
    }
}

function run() {
    takeConnection().setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
    // takeConnection().setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
    // takeConnection().setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
    
    // oldDelivery();
    newDelivery();
}

function fin() {
    if (getId() == 0) {
        // This block is performed only by Agent 0.
    }
}

// Application functions ---------------------------------------------

function oldDelivery() {
    var doPrint = true;
    var w_id = random(1, SCALE);
    
    for (var d_id = 1; d_id <= 10; d_id++) {
        var rs01 = fetchAsArray("SELECT /* D-01 */ n1.no_o_id "
                       + "FROM new_orders n1 "
                       + "WHERE n1.no_w_id = $int AND n1.no_d_id = $int "
                       + "AND n1.no_o_id = ("
                           + "SELECT MIN(n2.no_o_id) "
                           + "FROM new_orders n2 "
                           + "WHERE n2.no_w_id = $int AND n2.no_d_id = $int"
                       + ") "
                       + "FOR UPDATE",
                       w_id, d_id, w_id, d_id);
        
        if (rs01.length == 0) {
            info("[Agent " + getId() + "] SKIPPED , w : " + w_id + ", d : " + d_id);
            continue;
        } else if (doPrint) {
            debug("[Agent " + getId() + "] LOCKED  , w : " + w_id + ", d : " + d_id + ", o_id : " + rs01[0][0]);
            doPrint = false;
        }
        
        var uc02 = execute("DELETE /* D-02 */ "
                       + "FROM new_orders "
                       + "WHERE no_w_id = $int AND no_d_id = $int AND no_o_id = $int",
                       w_id, d_id, rs01[0][0]);
        
        var o_id = Number(rs01[0][0]) + 900;
        
        var uc03 = execute("INSERT INTO new_orders "
                       + "(no_o_id, no_d_id, no_w_id) "
                       + "VALUES ($int, $int, $int)",
                       o_id, d_id, w_id);
    }
    
    commit();
    debug("[Agent " + getId() + "] RELEASED, w : " + w_id);
}

function newDelivery() {
    var doPrint = true;
    var w_id = random(1, SCALE);
    
    warehouse: for (;;) {
        district: for (var d_id = 1; d_id <= 10; d_id++) {
            var rs01 = fetchAsArray("SELECT /* D-01 */ MIN(no_o_id) "
                        + "FROM new_orders "
                        + "WHERE no_w_id = $int AND no_d_id = $int",
                        w_id, d_id);
            
            if (rs01[0][0] == null) {
                info("[Agent " + getId() + "] NOT FOUND, w : " + w_id + ", d : " + d_id);
                continue district;
            } else if (doPrint) {
                debug("[Agent " + getId() + "] FETCHED , w : " + w_id + ", d : " + d_id + ", o_id : " + rs01[0][0]);
            }
            
            var uc02 = execute("DELETE /* D-02 */ "
                           + "FROM new_orders "
                           + "WHERE no_w_id = $int AND no_d_id = $int AND no_o_id = $int",
                           w_id, d_id, rs01[0][0]);
            
            if (uc02 == 0) {
                info("[Agent " + getId() + "] SKIPPED , w : " + w_id + ", d : " + d_id);
                rollback();
                doPrint = true;
                continue warehouse;
            } else if (doPrint) {
                debug("[Agent " + getId() + "] LOCKED  , w : " + w_id + ", d : " + d_id + ", o_id : " + rs01[0][0]);
                doPrint = false;
            }
            
            var o_id = Number(rs01[0][0]) + 900;
            
            var uc03 = execute("INSERT INTO new_orders "
                           + "(no_o_id, no_d_id, no_w_id) "
                           + "VALUES ($int, $int, $int)",
                           o_id, d_id, w_id);
        }
        
        break;
    }
    
    commit();
    debug("[Agent " + getId() + "] RELEASED, w : " + w_id);
}
