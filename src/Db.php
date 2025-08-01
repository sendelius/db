<?php

namespace Sendelius\Db;

use PDO;
use PDOException;

/**
 * Class Db
 * @package core\lib
 */
class Db {
	/**
	 * хранилище экземпляра класса SPDO
	 * @var SPDO
	 */
	public SPDO $pdo;

	/**
	 * префикс таблиц
	 * @var string
	 */
	public string $tablePrefix;

	/**
	 * режим дебага
	 * @var bool
	 */
	public bool $debug;

	/**
	 * описание структур таблиц
	 * @var array
	 */
	public array $tablesSchema;

	/**
	 * подключение к базе
	 */
	public function __construct(string $name, string $host = 'localhost', string $user = '', string $password = '', string $charset = 'utf8mb4', string $driver = 'mysql', string $port = '') {
        $driver = (in_array($driver,['mysql','pgsql'])) ? $driver : 'mysql';
		$options = ($driver === 'mysql')? [PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES " . $charset]: [];
		try {
			$dns = $driver.':dbname=' . $name . ';host=' . $host;
			if($port) $dns .= ';port=' . $port;
			$this->pdo = new SPDO($dns, $user, $password, $options);
		} catch (PDOException $e) {
			trigger_error("Ошибка подключения к базе данных: '" . $e->getMessage() . "'", E_USER_ERROR);
		}
	}

	/**
	 * выбор таблицы
	 * @param string $tableName
	 * @return Query
	 */
	public function table(string $tableName): Query {
		$table = $this->tablePrefix . $tableName;
		if(!array_key_exists($table, $this->tablesSchema)) {
			trigger_error("Таблица '.$tableName.' не зарегистрирована в системе", E_USER_ERROR);
		}
		$backtraceFile = '';
		$backtraceLine = 0;
		if ($this->debug) {
			$backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
			if (isset($backtrace[0])) {
				$backtraceFile = $backtrace[0]['file'];
				$backtraceLine = intval($backtrace[0]['line']);
			}
		}
		return new Query($this->pdo, $table, $this->tablesSchema[$table], $backtraceFile, $backtraceLine, $this->debug);
	}
}