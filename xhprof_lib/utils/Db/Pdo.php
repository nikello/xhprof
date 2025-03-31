<?php

/**
 * When setting the `id` column, consider the length of the prefix you're specifying in $this->prefix
 *
 *
 CREATE TABLE `details` (
 `id` char(17) NOT NULL,
 `url` varchar(255) default NULL,
 `c_url` varchar(255) default NULL,
 `timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
 `server name` varchar(64) default NULL,
 `perfdata` MEDIUMBLOB,
 `type` tinyint(4) default NULL,
 `cookie` BLOB,
 `post` BLOB,
 `get` BLOB,
 `pmu` int(11) unsigned default NULL,
 `wt` int(11) unsigned default NULL,
 `cpu` int(11) unsigned default NULL,
 `server_id` char(17) NOT NULL default 't11',
 `aggregateCalls_include` varchar(255) DEFAULT NULL,
 PRIMARY KEY  (`id`),
 KEY `url` (`url`),
 KEY `c_url` (`c_url`),
 KEY `cpu` (`cpu`),
 KEY `wt` (`wt`),
 KEY `pmu` (`pmu`),
 KEY `timestamp` (`timestamp`)
 ) ENGINE=MyISAM DEFAULT CHARSET=utf8;

 */

require_once XHPROF_LIB_ROOT.'/utils/Db/Abstract.php';

class Db_Pdo extends Db_Abstract {
    protected $curStmt;
    protected $dbType;
    protected $db;

    public function connect() {
        global $_xhprof;
        $this->dbType = $_xhprof['dbtype'] === 'mysql' ? 'mysql' : 'pgsql';
        $connectionString = $this->dbType . ':host=' . $this->config['dbhost'] . ';dbname=' . $this->config['dbname'];
        try {
            $this->db = new PDO($connectionString, $this->config['dbuser'], $this->config['dbpass']);
            $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->db->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
            if ($this->dbType === 'pgsql') {
                $this->db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false); // Preventing Binary Data from Being Converted to Strings
            }
        } catch (PDOException $e) {
            xhprof_error("Could not connect to db: " . $e->getMessage());
            throw new Exception("Unable to connect to database");
        }
    }

    public function prepare($sql) {
        if (!$this->db) {
            $this->connect();
        }
        return $this->db->prepare($sql);
    }

    public function query($sql) {
        if (!$this->db) {
            $this->connect();
        }
        $this->curStmt = $this->db->query($sql);
        return $this->curStmt;
    }

    public static function getNextAssoc($resultSet) {
        return $resultSet->fetch();
    }

    public function escape($str) {
        if (!$this->db) {
            $this->connect();
        }
        return substr($this->db->quote($str), 1, -1); // Remove quotes around strings
    }

    public function affectedRows() {
        return $this->curStmt ? $this->curStmt->rowCount() : 0;
    }

    public function unixTimestamp($field) {
        return $this->dbType === 'mysql' ? "UNIX_TIMESTAMP($field)" : "EXTRACT(EPOCH FROM $field)";
    }

    public function dateSub($days) {
        return $this->dbType === 'mysql' ? "DATE_SUB(CURDATE(), INTERVAL $days DAY)" : "CURRENT_DATE - INTERVAL '$days days'";
    }

    public function bindBinary($stmt, $param, $value) {
        if ($this->dbType === 'pgsql') {
            $stmt->bindValue($param, $value, PDO::PARAM_LOB);
        } else {
            $stmt->bindValue($param, $value, PDO::PARAM_STR); // MySQL treats binary data as strings
        }
    }
}