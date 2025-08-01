<?php

namespace Sendelius\Db;

use PDO;
use PDOStatement;

class SPDO extends PDO {
	/**
	 * массив запросов для дебага
	 * @var array
	 */
	public static array $queries = [];

	/**
	 * результат запроса
	 * @var bool
	 */
	public bool $result = false;

	/**
	 * запрос в базу
	 * @param string $sql
	 * @param array $data
	 * @param string $backtraceFile
	 * @param int $backtraceLine
	 * @param bool $debug
	 * @return bool|PDOStatement
	 */
	public function sendQuery(string $sql, array $data = [], string $backtraceFile = '', int $backtraceLine = 0, bool $debug = false): bool|PDOStatement {
		$time = 0;
		try {
			$start = microtime(true);
			$sqlObj = parent::prepare($sql);
			$this->result = $sqlObj->execute($data);
			$time = microtime(true) - $start;
		} catch (\Exception $exception) {
			trigger_error("Ошибка базы данных: " . $exception->getMessage(), E_USER_WARNING);
		}
		if ($debug) {
			$debugQueryString = $sql;
			foreach ($data as $key => $value) {
				if (is_null($value)) $repl = 'NULL';
				elseif (is_bool($value)) $repl = $value ? 'TRUE' : 'FALSE';
				elseif (is_numeric($value)) $repl = $value;
				else $repl = "'" . addslashes($value) . "'";
				$debugQueryString = str_replace($key, $repl, $debugQueryString);
			}
			self::$queries[] = [
				'query' => $debugQueryString,
				'time' => floatval(sprintf('%.6F', $time)),
				'file' => $backtraceFile,
				'line' => $backtraceLine,
			];
		}
		return $sqlObj;
	}
}