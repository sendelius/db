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
	public string $tablePrefix = '';

	/**
	 * режим дебага
	 * @var bool
	 */
	public bool $debug = false;

	/**
	 * описание структур таблиц
	 * @var array
	 */
	public array $tablesSchema =[];

	/**
	 * подключение к базе
	 * @param string $name
	 * @param string $host
	 * @param string $user
	 * @param string $password
	 * @param string $charset
	 * @param string $driver
	 * @param string $port
	 * @return void
	 */
	public function connect(string $name, string $host = 'localhost', string $user = '', string $password = '', string $charset = 'utf8mb4', string $driver = 'mysql', string $port = ''): void {
		$driver = (in_array($driver, ['mysql', 'pgsql'])) ? $driver : 'mysql';
		$options = ($driver === 'mysql') ? [PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES " . $charset] : [];
		try {
			$dns = $driver . ':dbname=' . $name . ';host=' . $host;
			if ($port) $dns .= ';port=' . $port;
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
		if (!array_key_exists($table, $this->tablesSchema)) {
			trigger_error("Таблица '.$tableName.' не зарегистрирована в системе", E_USER_WARNING);
			$tablesSchema = [];
		} else {
			$tablesSchema = $this->tablesSchema[$table];
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
		return new Query($this->pdo, $table, $tablesSchema, $backtraceFile, $backtraceLine, $this->debug);
	}

	/**
	 * регистрация таблицы в системе
	 * @param string|array $tables
	 * @return void
	 */
	public function registerTable(string|array $tables): void {
		if (!is_array($tables)) $tables = [$tables];
		foreach ($tables as $table) {
			if (class_exists($table)) {
				$tableClass = new $table();
				if (method_exists($tableClass, 'name') and method_exists($tableClass, 'schema')) {
					$schema = $tableClass->schema();
					$allowParams = [
						'type' => 'char',
						'length' => 0,
						'auto_increment' => false,
						'primary' => false,
						'unique' => false,
						'default' => '',
						'on_update' => '',
					];
					foreach ($schema as $key => $item) {
						$schema[$key] = $this->attrMerge($allowParams, $item);
					}
					$this->tablesSchema[$this->tablePrefix . $tableClass->name()] = $schema;
				}
			}
		}
	}

	/**
	 * функция слияния массивов с настройками
	 * @param array $defaults
	 * @param array $input
	 * @return array
	 */
	private function attrMerge(array $defaults, array $input): array {
		$result = [];
		foreach ($defaults as $key => $defaultValue) {
			if (array_key_exists($key, $input)) {
				if (is_array($defaultValue) && is_array($input[$key])) {
					$result[$key] = $this->attrMerge($defaultValue, $input[$key]);
				} else $result[$key] = $input[$key];
			} else $result[$key] = $defaultValue;
		}
		return $result;
	}
}