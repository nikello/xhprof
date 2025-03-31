<?php
//
//  Copyright (c) 2009 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//
// This file defines the interface iXHProfRuns and also provides a default
// implementation of the interface (class XHProfRuns).
//

/**
 * iXHProfRuns interface for getting/saving a XHProf run.
 *
 * Clients can either use the default implementation,
 * namely XHProfRuns_Default, of this interface or define
 * their own implementation.
 *
 * @author Kannan
 */
interface iXHProfRuns {

  /**
   * Returns XHProf data given a run id ($run) of a given
   * type ($type).
   *
   * Also, a brief description of the run is returned via the
   * $run_desc out parameter.
   */
  public function get_run($run_id, $type, &$run_desc);

  /**
   * Save XHProf data for a profiler run of specified type
   * ($type).
   *
   * The caller may optionally pass in run_id (which they
   * promise to be unique). If a run_id is not passed in,
   * the implementation of this method must generated a
   * unique run id for this saved XHProf run.
   *
   * Returns the run id for the saved XHProf run.
   *
   */
  public function save_run($xhprof_data, $type, $run_id = null);
}


/**
 * XHProfRuns_Default is the default implementation of the
 * iXHProfRuns interface for saving/fetching XHProf runs.
 *
 * This modified version of the file uses a MySQL backend to store
 * the data, it also stores additional information outside the run
 * itself (beyond simply the run id) to make comparisons and run
 * location easier
 * 
 * @author Kannan
 * @author Paul Reinheimer (http://blog.preinheimer.com)
 */
class XHProfRuns_Default implements iXHProfRuns {

    private $dir = '';
    public $prefix = 't11_';
    public $run_details = null;
    /**
     *
     * @var Db_Abstract
     */
    protected $db;

    public function __construct($dir = null) {
        $this->db();
    }

    protected function db() {
        global $_xhprof;
        require_once XHPROF_LIB_ROOT.'/utils/Db/'.$_xhprof['dbadapter'].'.php';

        $class = self::getDbClass();
        $this->db = new $class($_xhprof);
        $this->db->connect();
    }

    public static function getDbClass() {
        global $_xhprof;
        return 'Db_'.$_xhprof['dbadapter'];
    }

    private function gen_run_id($type) {
        return uniqid();
    }

    /**
     * This function gets runs based on passed parameters, column data as key, value as the value. Values
     * are escaped automatically. You may also pass limit, order by, group by, or "where" to add those values,
     * all of which are used as is, no escaping.
     *
     * @param array $stats Criteria by which to select columns
     * @return resource
     */
    public function getRuns($stats) {
        $query = isset($stats['select']) ? "SELECT {$stats['select']} FROM details " : "SELECT * FROM details ";
        $params = [];
        $skippers = ["limit", "order by", "group by", "where", "select"];
        $hasWhere = false;

        foreach ($stats as $column => $value) {
            if (in_array($column, $skippers)) continue;
            $query .= $hasWhere ? " AND " : " WHERE ";
            $hasWhere = true;
            $query .= strlen($value) == 0 ? "$column" : "$column = :$column";
            $params[":$column"] = $value;
        }

        if (isset($stats['where'])) {
            $query .= $hasWhere ? " AND " : " WHERE ";
            $query .= $stats['where'];
        }

        if (isset($stats['group by'])) $query .= " GROUP BY {$stats['group by']} ";
        if (isset($stats['order by'])) $query .= " ORDER BY {$stats['order by']} DESC ";
        if (isset($stats['limit'])) $query .= " LIMIT {$stats['limit']} ";

        $stmt = $this->db->prepare($query);
        $stmt->execute($params);
        return $stmt;
    }

    public function getHardHit($criteria) {
        $criteria['select'] = "DISTINCT({$criteria['type']}), COUNT({$criteria['type']}) AS count, SUM(wt) AS total_wall, AVG(wt) AS avg_wall";
        unset($criteria['type']);
        $criteria['where'] = $this->db->dateSub($criteria['days']) . " <= timestamp";
        unset($criteria['days']);
        $criteria['group by'] = "url";
        $criteria['order by'] = "count";
        return $this->getRuns($criteria);
    }

    public function getDistinct($data) {
        $column = $this->db->escape($data['column']);
        $query = "SELECT DISTINCT \"$column\" FROM details";
        return $this->db->query($query);
    }

    public static function getNextAssoc($resultSet) {
        return $resultSet->fetch();
    }

    public function get_run($run_id, $type, &$run_desc) {
        $stmt = $this->db->prepare("SELECT * FROM details WHERE id = :run_id");
        $stmt->execute([':run_id' => $run_id]);
        $data = $stmt->fetch();

        if (empty($data)) return [null, null];

        // Convert all binary columns from resources to strings
        $binaryColumns = ['perfdata', 'cookie', 'post', 'get'];
        foreach ($binaryColumns as $column) {
            if (isset($data[$column]) && is_resource($data[$column])) {
                $data[$column] = stream_get_contents($data[$column]);
            }
        }

        $contents = !isset($GLOBALS['_xhprof']['serializer']) || strtolower($GLOBALS['_xhprof']['serializer']) == 'php'
            ? unserialize(gzuncompress($data['perfdata']))
            : json_decode(gzuncompress($data['perfdata']), true);
        unset($data['perfdata']);

        $this->run_details = is_null($this->run_details) ? $data : [$this->run_details, $data];
        $run_desc = "XHProf Run (Namespace=$type)";
        $this->getRunComparativeData($data['url'], $data['c_url']);

        return [$contents, $data];
    }

    /**
     * Get stats (pmu, ct, wt) on a url or c_url
     *
     * @param array $data An associative array containing the limit you'd like to set for the queyr, as well as either c_url or url for the desired element.
     * @return resource result set from the database query
     */
    public function getUrlStats($data) {
        $data['select'] = "id, " . $this->db->unixTimestamp('timestamp') . " AS timestamp, pmu, wt, cpu";
        return $this->getRuns($data);
    }

    /**
     * Get comparative information for a given URL and c_url, this information will be used to display stats like how many calls a URL has,
     * average, min, max execution time, etc. This information is pushed into the global namespace, which is horribly hacky.
     *
     * @param string $url
     * @param string $c_url
     * @return array
     */
    public function getRunComparativeData($url, $c_url) {
        global $comparative;
        $metrics = "COUNT(id) AS \"count(`id`)\", AVG(wt) AS \"avg(`wt`)\", MIN(wt) AS \"min(`wt`)\", MAX(wt) AS \"max(`wt`)\", AVG(cpu) AS \"avg(`cpu`)\", MIN(cpu) AS \"min(`cpu`)\", MAX(cpu) AS \"max(`cpu`)\", AVG(pmu) AS \"avg(`pmu`)\", MIN(pmu) AS \"min(`pmu`)\", MAX(pmu) AS \"max(`pmu`)\"";

        $stmt = $this->db->prepare("SELECT $metrics FROM details WHERE url = :url");
        $stmt->execute([':url' => $url]);
        $row = $stmt->fetch();
        $row['url'] = $url;
        $row['95(`wt`)'] = $this->calculatePercentile(['count' => $row['count(`id`)'], 'column' => 'wt', 'type' => 'url', 'url' => $url]);
        $row['95(`cpu`)'] = $this->calculatePercentile(['count' => $row['count(`id`)'], 'column' => 'cpu', 'type' => 'url', 'url' => $url]);
        $row['95(`pmu`)'] = $this->calculatePercentile(['count' => $row['count(`id`)'], 'column' => 'pmu', 'type' => 'url', 'url' => $url]);
        $comparative['url'] = $row;

        $stmt = $this->db->prepare("SELECT $metrics FROM details WHERE c_url = :c_url");
        $stmt->execute([':c_url' => $c_url]);
        $row = $stmt->fetch();
        $row['url'] = $c_url;
        $row['95(`wt`)'] = $this->calculatePercentile(['count' => $row['count(`id`)'], 'column' => 'wt', 'type' => 'c_url', 'url' => $c_url]);
        $row['95(`cpu`)'] = $this->calculatePercentile(['count' => $row['count(`id`)'], 'column' => 'cpu', 'type' => 'c_url', 'url' => $c_url]);
        $row['95(`pmu`)'] = $this->calculatePercentile(['count' => $row['count(`id`)'], 'column' => 'pmu', 'type' => 'c_url', 'url' => $c_url]);
        $comparative['c_url'] = $row;

        return $comparative;
    }

    protected function calculatePercentile($details) {
        $limit = (int) ($details['count'] / 20);
        $stmt = $this->db->prepare("SELECT {$details['column']} AS value FROM details WHERE {$details['type']} = :url ORDER BY {$details['column']} DESC LIMIT 1 OFFSET :limit");
        $stmt->execute([':url' => $details['url'], ':limit' => $limit]);
        return $stmt->fetch()['value'];
    }

    /**
     * Save the run in the database.
     *
     * @param string $xhprof_data
     * @param mixed $type
     * @param string $run_id
     * @param mixed $xhprof_details
     * @return string
     */
    public function save_run($xhprof_data, $type, $run_id = null, $xhprof_details = null) {
        global $_xhprof;
        $run_id = $run_id ?? $this->gen_run_id($type);

        $sql = [
            'get' => !isset($_xhprof['serializer']) || strtolower($_xhprof['serializer']) == 'php' ? serialize($_GET) : json_encode($_GET),
            'cookie' => !isset($_xhprof['serializer']) || strtolower($_xhprof['serializer']) == 'php' ? serialize($_COOKIE) : json_encode($_COOKIE),
            'post' => (isset($_xhprof['savepost']) && $_xhprof['savepost'])
                ? (!isset($_xhprof['serializer']) || strtolower($_xhprof['serializer']) == 'php' ? serialize($_POST) : json_encode($_POST))
                : (!isset($_xhprof['serializer']) || strtolower($_xhprof['serializer']) == 'php' ? serialize(["Skipped" => "Post data omitted by rule"]) : json_encode(["Skipped" => "Post data omitted by rule"])),
            'pmu' => $xhprof_data['main()']['pmu'] ?? 0,
            'wt' => $xhprof_data['main()']['wt'] ?? 0,
            'cpu' => $xhprof_data['main()']['cpu'] ?? 0,
            'data' => !isset($_xhprof['serializer']) || strtolower($_xhprof['serializer']) == 'php' ? gzcompress(serialize($xhprof_data), 2) : gzcompress(json_encode($xhprof_data), 2),
            'url' => PHP_SAPI === 'cli' ? implode(' ', $_SERVER['argv']) : $_SERVER['REQUEST_URI'],
            'c_url' => _urlSimilartor(PHP_SAPI === 'cli' ? implode(' ', $_SERVER['argv']) : $_SERVER['REQUEST_URI']),
            'servername' => $_SERVER['SERVER_NAME'] ?? '',
            'type' => $xhprof_details['type'] ?? 0,
            'timestamp' => $_SERVER['REQUEST_TIME'],
            'server_id' => $_xhprof['servername'],
            'aggregateCalls_include' => getenv('xhprof_aggregateCalls_include') ?: ''
        ];

        $query = "INSERT INTO details (id, url, c_url, timestamp, \"server name\", perfdata, type, cookie, post, get, pmu, wt, cpu, server_id, \"aggregateCalls_include\")
                  VALUES (:id, :url, :c_url, to_timestamp(:timestamp), :servername, :data, :type, :cookie, :post, :get, :pmu, :wt, :cpu, :server_id, :aggregateCalls_include)";
        $stmt = $this->db->prepare($query);

        $stmt->bindValue(':id', $run_id, PDO::PARAM_STR);
        $stmt->bindValue(':url', $sql['url'], PDO::PARAM_STR);
        $stmt->bindValue(':c_url', $sql['c_url'], PDO::PARAM_STR);
        $stmt->bindValue(':timestamp', $sql['timestamp'], PDO::PARAM_INT);
        $stmt->bindValue(':servername', $sql['servername'], PDO::PARAM_STR);
        $this->db->bindBinary($stmt, ':data', $sql['data']);
        $stmt->bindValue(':type', $sql['type'], PDO::PARAM_INT);
        $this->db->bindBinary($stmt, ':cookie', $sql['cookie']);
        $this->db->bindBinary($stmt, ':post', $sql['post']);
        $this->db->bindBinary($stmt, ':get', $sql['get']);
        $stmt->bindValue(':pmu', $sql['pmu'], PDO::PARAM_INT);
        $stmt->bindValue(':wt', $sql['wt'], PDO::PARAM_INT);
        $stmt->bindValue(':cpu', $sql['cpu'], PDO::PARAM_INT);
        $stmt->bindValue(':server_id', $sql['server_id'], PDO::PARAM_STR);
        $stmt->bindValue(':aggregateCalls_include', $sql['aggregateCalls_include'], PDO::PARAM_STR);

        $stmt->execute();

        return $stmt->rowCount() == 1 ? $run_id : -1;
    }
}

