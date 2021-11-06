<?php
/**
 * @project Sendelius Db
 * @author Sendelius <sendelius@gmail.com>
 */

namespace SendeliusDb;

use PDO;
use PDOException;

/**
 * Class Db
 * @package SendeliusDb
 * @property PDOext $sql
 */
class Db{
	protected $sql;
	protected $query = null;
    protected $cquery = null;
	protected $first = null;
	protected $where = null;
	protected $limit = null;
	protected $order = null;
	protected $group = null;
	protected $data = array();
	protected static $whereKeyIndex = 0;
	protected static $_sqlInstance;

	public $table;
	public $queryString;
	public $executeResult;
	public $executeData;

	public static $lastQueryString;
	public static $lastExecuteResult;
	public static $lastExecuteData;

	public function __construct($newInstance=false){
		if($newInstance === true) self::$_sqlInstance = null;
	}

	public function connect($db_name,$host,$user,$password,$table,$table_prefix=null,$charset='utf8',$driver='mysql'){
		$this->table = $table_prefix.$table;
		$dsn = $driver.':dbname='.$db_name.';host='.$host;
		try {
			if (null === self::$_sqlInstance){
				self::$_sqlInstance = new PDOext($dsn, $user, $password, array(PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES ".$charset));
			}
			$this->sql = self::$_sqlInstance;
			$this->sql->table = $this->table;
			$this->sql->driver = $driver;
		} catch (PDOException $e) {
			trigger_error("Error connecting to database: '".$e->getMessage()."'",E_USER_ERROR);
		}
	}

	protected function setData($data=array()){
		$this->data = array_merge($this->data,$data);
	}

	protected function fastQuery(){
		if($this->first!=null) $this->query = $this->first;
		if($this->where!=null) $this->query .= ' '.$this->where;
		if($this->group!=null) $this->query .= ' '.$this->group;
		if($this->order!=null) $this->query .= ' '.$this->order;
		if($this->limit!=null) $this->query .= ' '.$this->limit;
		$this->first = null;
		$this->where = null;
		$this->group = null;
		$this->order = null;
		$this->limit = null;
		$result = $this->sql->query($this->query,$this->data);
		$this->queryString = $result->queryString;
		self::$lastQueryString = $result->queryString;
		$this->executeResult = $this->sql->executeResult;
		self::$lastExecuteResult = $this->sql->executeResult;
		$this->executeData = $this->sql->executeData;
		self::$lastExecuteData = $this->sql->executeData;
		$this->data = array();
		return $result;
	}

	protected function genWhere($key,$cond,$value){
		$where = null;
		if(strtoupper($cond)=='IN') {
			if(is_array($value) AND count($value)>0){
				$aKeys = array_keys($value);
				$newValues = array_combine(preg_replace('/^/',':where_in_'.self::$whereKeyIndex.'_',$aKeys,1),$value);
				$where = $key.' IN('.implode(',',array_keys($newValues)).')';
				$this->setData($newValues);
				self::$whereKeyIndex = self::$whereKeyIndex+1;
			}
		}elseif(strtoupper($cond)=='BETWEEN'){
			if(is_array($value) AND count($value)>0){
				$aKeys = array_keys($value);
				$newValues = array_combine(preg_replace('/^/',':where_bw_'.self::$whereKeyIndex.'_',$aKeys,1),$value);
				$where = '('.$key.' BETWEEN '.implode(' AND ',array_keys($newValues)).')';
				$this->setData($newValues);
				self::$whereKeyIndex = self::$whereKeyIndex+1;
			}
		}elseif(strtoupper($cond)=='CQUERY'){
			$where = $key;
		}elseif(strtoupper($cond)=='EXISTS'){
			$where = $cond.' '.$key;
		}elseif(strtoupper($cond)=='NOT EXISTS'){
			$where = $cond.' '.$key;
		}elseif(strtoupper($cond)=='NULL' OR strtoupper($cond)=='IS NULL'){
			$where = $key.' IS NULL';
		}elseif(strtoupper($cond)=='NOT NULL' OR strtoupper($cond)=='IS NOT NULL'){
			$where = $key.' IS NOT NULL';
		}elseif(strtoupper($cond)=='MATCH'){
			if(is_array($key) AND strlen($value)>0){
				$search = explode(' ',htmlspecialchars(trim($value),ENT_QUOTES));
				$against = implode('* +',$search);
				$where = $cond.' ('.implode(',',$key).') AGAINST (\'+'.$against.'*\' IN BOOLEAN MODE)';
			}
        }elseif(strtoupper($cond)=='LIKE'){
            $where = $key.' '.$cond.' \''.$value.'\'';
		}else{
			$newKey = preg_replace('/^/',':where_'.self::$whereKeyIndex.'_',$key,1);
			$where = $key.' '.$cond.' '.$newKey;
			$this->setData(array($newKey=>$value));
			self::$whereKeyIndex = self::$whereKeyIndex+1;
		}
		return $where;
	}

	public function multiWhere($cond=array()){
		$where = null;
		$i = 1;
		$count = count($cond);
		foreach($cond as $val){
			$where .= $this->genWhere($val[0],$val[1],$val[2]).' ';
			if(isset($val[3]) and $i<$count){
				$where .= $val[3].' ';
			}
            $i++;
		}
		if($this->where!=null){
			$this->where .= ' '.$where;
		}else{
			$this->where = $this->cquery.' WHERE '.$where;
		}
		return $this;
	}

    public function cquery($value){
        $this->cquery .= ' '.$value;
        return $this;
    }

    public function emptyWhere(){
        $this->where = $this->cquery.' ';
        return $this;
    }

	public function where($key,$cond,$value){
		$where = $this->genWhere($key,$cond,$value);
		if($this->where!=null){
			$this->where .= ' '.$where;
		}else{
			$this->where = $this->cquery.' WHERE '.$where;
		}
		return $this;
	}

	public function _and(){
		if($this->where!=null){
			$this->where .= ' AND';
		}
		return $this;
	}

	public function _or(){
		if($this->where!=null){
			$this->where .= ' OR';
		}
		return $this;
	}

	public function _startGroup(){
		if($this->where!=null){
			$this->where .= ' (';
		}
		return $this;
	}

	public function _endGroup(){
		if($this->where!=null){
			$this->where .= ')';
		}
		return $this;
	}

	public function search(array $columns,$search){
		if(count($columns)>0){
			$q = array();
			foreach($columns as $c){
				$r = explode(' ', trim($search));
				$r = implode('|', $r);
				$q[] = '(LOWER('.$c.') REGEXP LOWER(\''.$r.'\'))';
			}
			if($this->where!=null){
				$this->where .= ' AND '.$this->genWhere('('.implode(' OR ',$q).')','CQUERY',null);
			}else{
				$this->where = $this->cquery.' WHERE '.$this->genWhere('('.implode(' OR ',$q).')','CQUERY',null);
			}
		}
		return $this;
	}

	public function getAll($columns='*'){
		$this->first = 'SELECT '.$columns.' FROM '.$this->table;
		$result = $this->fastQuery();
		return $result->fetchAll(PDO::FETCH_ASSOC);
	}

	public function get($columns='*'){
		$this->first = 'SELECT '.$columns.' FROM '.$this->table;
		$result = $this->fastQuery();
		return $result->fetch(PDO::FETCH_ASSOC);
	}

	public function insert($values=array()){
		if(count($values)>0){
			$aKeys = array_keys($values);
			$newValues = array_combine(preg_replace('/^/',':',$aKeys,1),$values);
			$this->first = "INSERT INTO ".$this->table." (".implode(',',$aKeys).") VALUE (".implode(',',array_keys($newValues)).")";
			$this->setData($newValues);
			$this->fastQuery();
			if($this->executeResult!=false){
				return $this->sql->lastInsertId();
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	public function multiInsert($values=array(),$limit=1000){
		$count = count($values);
		if($count>0){
            $aKeys = array_keys($values[0]);
            $values_chunk = array_chunk($values, $limit, true);
            foreach ($values_chunk as $values_array) {
                $query = "INSERT INTO ".$this->table." (".implode(',',$aKeys).") VALUES ";
                $i=1;
                foreach($values_array as $value){
                    $newValues = array_combine(preg_replace('/^/',':'.$i,$aKeys,1),$value);
                    $query .= "(".implode(',',array_keys($newValues)).")";
                    $this->setData($newValues);
                    if($count!=$i) $query .= ",";
                    $i++;
                }
                $this->first = $query;
                $this->fastQuery();
            }
			if($this->executeResult!=false){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	public function update($values=array()){
		if(count($values)>0){
			$aKeys = array_keys($values);
			$newValues = array_combine(preg_replace('/^/',':',$aKeys,1),$values);
			$newKeys = array_combine($aKeys,preg_replace('/^/',':',$aKeys,1));
			$set = null;
			$i=1;
			$count = count($newKeys);
			foreach($newKeys as $k => $v){
				$set .= "{$k} = {$v}";
				if($i<$count){
					$set .= ", ";
				}
				$i++;
			}
			$this->first = "UPDATE ".$this->table." SET ".$set;
			$this->setData($newValues);
			return $this->fastQuery();
		}else{
			return false;
		}
	}

    public function multiUpdate($values=array(),$limit=1000){
        $count = count($values);
        if($count>0){
            $values_chunk = array_chunk($values, $limit, true);
            foreach ($values_chunk as $values_array){
                foreach ($values_array as $value){
                    $aKeys = array_keys($value);
                    $newValues = array_combine(preg_replace('/^/',':',$aKeys,1),$value);
                    $newKeys = array_combine($aKeys,preg_replace('/^/',':',$aKeys,1));
                    $set = null;
                    $i=1;
                    $count = count($newKeys);
                    $key_first = false;
                    foreach($newKeys as $k => $v){
                        if($i==1) {
                            $key_first = "WHERE {$k} = {$v}";
                        }else{
                            $set .= "{$k} = {$v}";
                            if($i<$count){
                                $set .= ", ";
                            }
                        }
                        $i++;
                    }
                    if($key_first){
                        $this->first = "UPDATE ".$this->table." SET ".$set." ".$key_first;
                        $this->setData($newValues);
                        $this->fastQuery();
                    }
                }
            }
            if($this->executeResult!=false){
                return true;
            }else{
                return false;
            }
        }else{
            return false;
        }
    }

	public function delete(){
		$this->first = "DELETE FROM ".$this->table;
		return $this->fastQuery();
	}

	public function truncate(){
		$this->first = "TRUNCATE TABLE ".$this->table;
		return $this->fastQuery();
	}

	public function exist(){
		$this->first = "SELECT * FROM ".$this->table;
		$result = $this->fastQuery();
		if($this->executeResult!=false){
			if($result->rowCount()>0){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	public function count(){
		$this->first = "SELECT * FROM ".$this->table;
		$result = $this->fastQuery();
		return $result->rowCount();
	}

	public function group($column){
		$this->group = "GROUP BY ".$column;
		return $this;
	}

	public function limit($offset=0,$rows=null){
		if($rows!=null)$rows = ','.$rows;
		$this->limit = "LIMIT ".$offset.$rows;
		return $this;
	}

	public function order($column,$order=null){
		if($order!=null)$order = ' '.$order;
		$this->order = "ORDER BY ".$column.$order;
		return $this;
	}

	public function rand(){
		$this->order = "ORDER BY RAND()";
		return $this;
	}
}
