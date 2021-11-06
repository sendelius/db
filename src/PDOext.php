<?php
/**
 * @project Sendelius Db
 * @author Sendelius <sendelius@gmail.com>
 */

namespace SendeliusDb;

use PDO;

/**
 * Class PDOext
 * @package SendeliusDb
 */
class PDOext extends PDO{
	public $table = null;
	public $driver = null;
	public $executeResult;
	public $executeData;
	public function query($sql,array $data = array()){
		$sql = parent::prepare($sql);
		$this->executeResult = $sql->execute($data);
		$this->executeData = $data;
		if($this->executeResult!=true){
			$errorInfo = $sql->errorInfo();
			trigger_error($this->driver." error: '".$errorInfo[2]."'");
		}
		return $sql;
	}
}