<?php

namespace Sendelius\Db;

use PDO;
use PDOStatement;

class Query {
	/**
	 * Хранилище экземпляра класса SPDO
	 * @var SPDO
	 */
	private SPDO $pdo;

	/**
	 * Таблица для запроса
	 * @var string
	 */
	private string $tableName;

	/**
	 * Описание структуры таблицы
	 * @var array
	 */
	private array $tableSchema;

	/**
	 * Хранилище данных
	 * @var array
	 */
	private array $data = [];

	/**
	 * Содержимое оператора join
	 * @var array
	 */
	private array $join = [];

	/**
	 * Содержимое оператора WHERE
	 * @var array
	 */
	private array $where = [];

	/**
	 * Содержимое оператора LIMIT
	 * @var string
	 */
	private string $limit = '';

	/**
	 * Содержимое оператора ORDER
	 * @var string
	 */
	private string $order = '';

	/**
	 * Содержимое оператора GROUP
	 * @var string
	 */
	private string $group = '';

	/**
	 * Содержимое оператора HAVING
	 * @var string
	 */
	private string $having = '';

	/**
	 * Индекс ключа данных для оператора WHERE
	 * @var int
	 */
	private static int $whereKeyIndex = 0;

	/**
	 * Файл вызова
	 * @var string
	 */
	private string $backtraceFile;

	/**
	 * Строка вызова
	 * @var int
	 */
	private int $backtraceLine;

	/**
	 * Режим дебага
	 * @var bool
	 */
	public bool $debug;

	/**
	 * Текущий драйвер соединения
	 * @var string|mixed
	 */
	private string $driver;

	/**
	 * Экранирующий символ
	 * @var string
	 */
	private string $sld;

	/**
	 * Конструктор класса
	 * @param SPDO $pdo
	 * @param string $tableName
	 * @param array $tableSchema
	 * @param string $backtraceFile
	 * @param int $backtraceLine
	 * @param bool $debug
	 */
	public function __construct(SPDO $pdo, string $tableName = '', array $tableSchema = [], string $backtraceFile = '', int $backtraceLine = 0, bool $debug = false) {
		$this->pdo = $pdo;
		$this->tableSchema = $tableSchema;
		$this->tableName = $tableName;
		$this->backtraceFile = $backtraceFile;
		$this->backtraceLine = $backtraceLine;
		$this->debug = $debug;
		$this->driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
		$this->sld = ($this->driver == 'pgsql') ? '"' : '`';
	}

	/**
	 * Экранирование названий колонок
	 * @param string $name
	 * @return string
	 */
	private function shield(string $name): string {
		return $this->sld . $name . $this->sld;
	}

	/**
	 * Оператор JOIN
	 * @param string $type
	 * @param string $table
	 * @param array $on
	 * @return $this
	 */
	public function join(string $type, string $table, array $on): Query {
		$type = (in_array(strtolower($type), ['left', 'right'])) ? $type : 'LEFT';
		$joinTemp = strtoupper($type) . ' JOIN ' . $table . ' ON';
		foreach ($on as $val) {
			$joinTemp .= ' ' . $val;
		}
		$this->join[] = $joinTemp;
		return $this;
	}

	/**
	 * Оператор HAVING
	 * @param string $key
	 * @param string $cond
	 * @param string $value
	 * @return $this
	 */
	public function having(string $key, string $cond, string $value): Query {
		$this->having = 'HAVING COUNT(' . $this->shield($key) . ') ' . $cond . ' ' . $value;
		return $this;
	}

	/**
	 * Оператор WHERE
	 * @param mixed $key
	 * @param string $cond
	 * @param mixed $value
	 * @param string $operator доступные значения: and или or
	 * @return $this
	 */
	public function where(mixed $key, string $cond, mixed $value = '', string $operator = 'AND'): Query {
		$where = $this->genWhere($key, $cond, $value);
		if (strlen($where) > 0) {
			$this->where[] = $where;
			$this->where[] = match (strtolower($operator)) {
				'or' => 'OR',
				default => 'AND',
			};
		}
		return $this;
	}

	/**
	 * Множественный оператор WHERE
	 * @param array $cond
	 * @param bool $group
	 * @param string $endOperator
	 * @return $this
	 */
	public function multiWhere(array $cond = [], bool $group = false, string $endOperator = 'AND'): Query {
		if ($group and count($cond) > 0) $this->where[] = '(';
		foreach ($cond as $val) {
			if (count($val) > 2 and isset($val[0]) and isset($val[1]) and isset($val[2])) {
				$where = $this->genWhere($val[0], $val[1], $val[2]);
				if (strlen($where) > 0) {
					$this->where[] = $where;
					if (isset($val[3])) {
						$this->where[] = match (strtolower($val[3])) {
							'or' => 'OR',
							default => 'AND',
						};
					} else $this->where[] = 'AND';
				}
			}
		}
		if ($group and count($cond) > 0) {
			$endElement = array_pop($this->where);
			if ($endElement != 'AND' and $endElement != 'OR') $this->where[] = $endElement;
			$this->where[] = ')';
			$this->where[] = match (strtolower($endOperator)) {
				'or' => 'OR',
				default => 'AND',
			};
		}
		return $this;
	}

	/**
	 * Поиск по базе данных
	 * @param array $columns
	 * @param string $search
	 * @param bool $explode
	 * @return $this
	 */
	public function search(array $columns = [], string $search = '', bool $explode = true): Query {
		if (count($columns) > 0 and strlen($search) > 0) {
			if ($explode) {
				$searchArray = explode(' ', trim($search));
				foreach ($searchArray as $key => $item) $searchArray[$key] = trim($item);
				$search = implode('|', $searchArray);
			}

			$query = [];
			foreach ($columns as $column) $query[] = '(LOWER(' . $this->shield($column) . ') REGEXP LOWER(\'' . $search . '\'))';

			$this->where('(' . implode(' OR ', $query) . ')', 'CUSTOM');
		}
		return $this;
	}

	/**
	 * Выбор количества строк в таблице
	 * @param string $key
	 * @return int
	 */
	public function count(string $key = '*'): int {
		$result = $this->sendQuery('SELECT COUNT(' . $key . ') as count_items FROM ' . $this->tableName);
		$resultFetch = $result->fetch(PDO::FETCH_ASSOC);
		return (isset($resultFetch['count_items'])) ? (int)$resultFetch['count_items'] : 0;
	}

	/**
	 * Группировка выборки по столбцу
	 * @param string $column
	 * @return $this
	 */
	public function group(string $column): Query {
		$this->group = "GROUP BY " . $column;
		return $this;
	}

	/**
	 * Установка лимита выборки
	 * @param int $offset
	 * @param int $rows
	 * @return $this
	 */
	public function limit(int $rows = 0, int $offset = 0): Query {
		if ($this->driver === 'pgsql') {
			$this->limit = "LIMIT $rows";
			if ($offset > 0) $this->limit .= " OFFSET $offset";
		} else {
			if ($offset > 0) $this->limit = "LIMIT " . $offset . "," . $rows;
			else $this->limit = "LIMIT " . $rows;
		}
		return $this;
	}

	/**
	 * Сортировка
	 * @param string $column
	 * @param string $order ASC or DESC
	 * @return $this
	 */
	public function order(string $column, string $order = ''): Query {
		$order = trim(strtoupper($order));
		if (!in_array($order, ['ASC', 'DESC'])) $order = '';
		if (strlen($column) > 0) {
			$this->order = "ORDER BY " . $column;
			$this->order .= (strlen($order) > 0) ? ' ' . $order : $order;
		}
		return $this;
	}

	/**
	 * Случайная сортировка
	 * @return $this
	 */
	public function rand(): Query {
		if ($this->driver === 'pgsql') $this->order = "ORDER BY RANDOM()";
		else $this->order = "ORDER BY RAND()";
		return $this;
	}

	/**
	 * Выборка нескольких строк
	 * @param array|string $columns
	 * @param array $as
	 * @param bool $asReverse
	 * @return array
	 */
	public function getAll(array|string $columns = [], array $as = [], bool $asReverse = false): array {
		$result = $this->sendQuery('SELECT ' . $this->prepareColumns($columns, $as, $asReverse) . ' FROM ' . $this->tableName);
		$rows = $result->fetchAll(PDO::FETCH_ASSOC);
		return array_map([$this, 'prepareResult'], $rows);
	}

	/**
	 * Выборка одной строки
	 * @param array|string $columns
	 * @param array $as
	 * @param bool $asReverse
	 * @return array|null
	 */
	public function get(array|string $columns = [], array $as = [], bool $asReverse = false): ?array {
		$result = $this->sendQuery('SELECT ' . $this->prepareColumns($columns, $as, $asReverse) . ' FROM ' . $this->tableName);
		$row = $result->fetch(PDO::FETCH_ASSOC);
		return $row ? $this->prepareResult($row) : null;
	}

	/**
	 * Создание записей в базе
	 * @param array $values
	 * @return false|string
	 */
	public function insert(array $values = []): bool|string {
		if (count($values) > 0) {
			$aKeys = array_keys($values);
			$newValues = array_combine(preg_replace('/^/', ':', $aKeys, 1), $values);
			$this->setData($newValues);
			if ($this->driver === 'pgsql') {
				$query = sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id", $this->tableName, $this->shield(implode($this->shield(','), $aKeys)), implode(',', array_keys($newValues)));
			} else {
				$query = sprintf("INSERT INTO %s (%s) VALUE (%s)", $this->tableName, $this->shield(implode($this->shield(','), $aKeys)), implode(',', array_keys($newValues)));
			}
			$this->sendQuery($query);
			if ($this->pdo->result !== false) {
				return $this->pdo->lastInsertId();
			} else return false;
		} else return false;
	}

	/**
	 * Обновление записей в базе
	 * @param array $values
	 * @return bool|PDOStatement
	 */
	public function update(array $values = []): bool|PDOStatement {
		if (count($values) > 0) {
			if ($this->driver === 'pgsql') {
				foreach ($this->tableSchema as $column => $definition) {
					if (!array_key_exists($column, $values) && strlen((string)$definition['on_update'])) {
						$values[$column] = $definition['on_update'];
					}
				}
			}

			$aKeys = array_keys($values);
			$newValues = array_combine(preg_replace('/^/', ':', $aKeys, 1), $values);
			$newKeys = array_combine($aKeys, preg_replace('/^/', ':', $aKeys, 1));
			$set = [];
			foreach ($newKeys as $k => $v) $set[] = $this->shield($k) . " = $v";
			$this->setData($newValues);
			return $this->sendQuery("UPDATE " . $this->tableName . " SET " . implode(', ', $set));
		} else return false;
	}

	/**
	 * Удаление записей из базы данных
	 * @return bool|PDOStatement
	 */
	public function delete(): bool|PDOStatement {
		return $this->sendQuery("DELETE FROM " . $this->tableName);
	}

	/**
	 * Отчистка таблицы
	 * @return bool|PDOStatement
	 */
	public function truncate(): bool|PDOStatement {
		return $this->sendQuery("TRUNCATE TABLE " . $this->tableName);
	}

	/**
	 * Множественное создание записей
	 * @param array $values
	 * @param int $limit
	 * @return bool
	 */
	public function multiInsert(array $values = [], int $limit = 1000): bool {
		$count = count($values);
		if ($count > 0) {
			$aKeys = array_keys($values[0]);
			$valuesChunk = array_chunk($values, $limit, true);
			foreach ($valuesChunk as $valuesArray) {
				$query = "INSERT INTO " . $this->tableName . " (" . $this->shield(implode($this->shield(','), $aKeys)) . ") VALUES ";
				$queryValues = [];
				$i = 1;
				foreach ($valuesArray as $value) {
					$value = array_replace(array_flip($aKeys), $value);
					$newValues = array_combine(preg_replace('/^/', ':' . $i, $aKeys, 1), $value);
					$queryValues[] = "(" . implode(',', array_keys($newValues)) . ")";
					$this->setData($newValues);
					$i++;
				}
				$query .= implode(',', $queryValues);
				$this->sendQuery($query);
			}
			if ($this->pdo->result !== false) return true;
			else return false;
		} else return false;
	}

	/**
	 * Множественное обновление записей
	 * @param array $values
	 * @param int $limit
	 * @return bool
	 */
	public function multiUpdate(array $values = [], int $limit = 1000): bool {
		$count = count($values);
		if ($count > 0) {
			$valuesChunk = array_chunk($values, $limit, true);
			foreach ($valuesChunk as $valuesArray) {
				foreach ($valuesArray as $value) {
					$aKeys = array_keys($value);
					$newValues = array_combine(preg_replace('/^/', ':', $aKeys, 1), $value);
					$newKeys = array_combine($aKeys, preg_replace('/^/', ':', $aKeys, 1));
					$set = [];
					$i = 1;
					$keyFirst = false;
					foreach ($newKeys as $k => $v) {
						if ($i == 1) $keyFirst = "WHERE " . $this->shield($k) . " = $v";
						else $set[] = $this->shield($k) . " = $v";
						$i++;
					}
					if ($keyFirst and count($set) > 0) {
						$this->setData($newValues);
						$this->sendQuery("UPDATE " . $this->tableName . " SET " . implode(',', $set) . " " . $keyFirst);
					}
				}
			}
			if ($this->pdo->result !== false) return true;
			else return false;
		} else return false;
	}

	/**
	 * Произвольный sql запрос
	 * @param string $query
	 * @return bool|PDOStatement
	 */
	public function customQuery(string $query): bool|PDOStatement {
		return $this->sendQuery($query);
	}

	/**
	 * Добавление данных в хранилище
	 * @param array $data
	 */
	private function setData(array $data = []): void {
		$this->data = array_merge($this->data, $data);
	}

	/**
	 * Генерация содержимого оператора WHERE
	 * @param mixed $key
	 * @param string $cond
	 * @param mixed $value
	 * @return string
	 */
	private function genWhere(mixed $key, string $cond, mixed $value = ''): string {
		$where = '';
		$value = $this->prepareValues($key, $value);
		switch (strtoupper($cond)) {
			case 'IN':
				if (is_array($value) and count($value) > 0) {
					$aKeys = array_keys($value);
					$newValues = array_combine(preg_replace('/^/', ':where_in_' . self::$whereKeyIndex . '_', $aKeys, 1), $value);
					$where = $this->shield($key) . ' IN(' . implode(',', array_keys($newValues)) . ')';
					$this->setData($newValues);
					self::$whereKeyIndex++;
				}
				break;
			case 'JSON_CONTAINS':
				if (is_array($value) and count($value) > 0) {
					$aKeys = array_keys($value);
					$newValues = array_combine(preg_replace('/^/', ':where_in_' . self::$whereKeyIndex . '_', $aKeys, 1), $value);
					$where = 'JSON_CONTAINS(' . $this->shield($key) . ', ' . implode(',', array_keys($newValues)) . ')';
					$this->setData($newValues);
					self::$whereKeyIndex++;
				}
				break;
			case 'NOT IN':
				if (is_array($value) and count($value) > 0) {
					$aKeys = array_keys($value);
					$newValues = array_combine(preg_replace('/^/', ':where_in_' . self::$whereKeyIndex . '_', $aKeys, 1), $value);
					$where = $this->shield($key) . ' NOT IN(' . implode(',', array_keys($newValues)) . ')';
					$this->setData($newValues);
					self::$whereKeyIndex++;
				}
				break;
			case 'BETWEEN':
				if (is_array($value) and count($value) > 0) {
					$aKeys = array_keys($value);
					$newValues = array_combine(preg_replace('/^/', ':where_bw_' . self::$whereKeyIndex . '_', $aKeys, 1), $value);
					$where = '(' . $key . ' BETWEEN ' . implode(' AND ', array_keys($newValues)) . ')';
					$this->setData($newValues);
					self::$whereKeyIndex++;
				}
				break;
			case 'MATCH':
				if (is_array($key) and strlen((string)$value) > 0) {
					$search = explode(' ', htmlspecialchars(trim($value), ENT_QUOTES));
					$against = implode('* +', $search);
					$where = $cond . ' (' . $this->shield(implode($this->shield(','), $key)) . ') AGAINST (\'+' . $against . '*\' IN BOOLEAN MODE)';
				}
				break;
			case 'LIKE':
			case 'NOT LIKE':
			case 'REGEXP':
				$where = $this->shield($key) . ' ' . $cond . ' \'' . $value . '\'';
				break;
			case 'CUSTOM':
				$where = $key;
				break;
			case 'EXISTS':
			case 'NOT EXISTS':
				$where = $cond . ' ' . $this->shield($key);
				break;
			case 'NULL':
			case 'IS NULL':
				$where = $this->shield($key) . ' IS NULL';
				break;
			case 'NOT NULL':
			case 'IS NOT NULL':
				$where = $this->shield($key) . ' IS NOT NULL';
				break;
			case 'LENGTH>':
				$where = 'LENGTH(' . $this->shield($key) . ') > \'' . $value . '\'';
				break;
			case 'LENGTH<':
				$where = 'LENGTH(' . $this->shield($key) . ') < \'' . $value . '\'';
				break;
			case 'LENGTH=':
				$where = 'LENGTH(' . $this->shield($key) . ') = \'' . $value . '\'';
				break;
			default:
				$newKey = preg_replace('/^/', ':where_' . self::$whereKeyIndex . '_', $key, 1);
				$where = $this->shield($key) . ' ' . $cond . ' ' . $newKey;
				$this->setData([$newKey => $value]);
				self::$whereKeyIndex++;
				break;
		}
		return (string)$where;
	}

	/**
	 * Форматирование данных под их типы
	 * @param array $row
	 * @return array
	 */
	private function prepareResult(array $row): array {
		foreach ($row as $key => $value) {
			if (!isset($this->tableSchema[$key]) || $value === null) {
				continue;
			}
			switch (strtolower($this->tableSchema[$key]['type'])) {
				case 'boolean':
					$row[$key] = (bool)$value;
					break;
				case 'json':
					$row[$key] = (json_validate((string)$value)) ? json_decode((string)$value, true) : [];
					break;
				case 'int':
				case 'bigint':
					$row[$key] = (int)$value;
					break;
				// можно добавить другие типы по необходимости
			}
		}
		return $row;
	}

	/**
	 * Подготовка значений по схеме таблицы
	 * @param mixed $key
	 * @param mixed $value
	 * @return mixed
	 */
	private function prepareValues(mixed $key, mixed $value): mixed {
		if (array_key_exists($key, $this->tableSchema) and $this->tableSchema[$key]['type'] == 'boolean') {
			if (is_bool($value) and $this->driver !== 'pgsql') $value = ($value) ? 1 : 0;
			if (!is_bool($value) and $this->driver === 'pgsql') $value = (bool)$value;
		}
		return $value;
	}

	/**
	 * Подготовка данных по колонкам для выборки
	 * @param array|string $columns
	 * @param array $as
	 * @param bool $asReverse
	 * @return string
	 */
	private function prepareColumns(array|string $columns = [], array $as = [], bool $asReverse = false): string {
		if (is_array($columns)) $columnsString = (count($columns) > 0) ? $this->sld . implode($this->sld . ',' . $this->sld, $columns) . $this->sld : '*';
		else $columnsString = (is_string($columns) and strlen($columns) > 0) ? $columns : '*';

		if (count($as) > 0) {
			foreach ($as as $asKey => $asKeyReplace) {
				$columnsString = ($asReverse) ? str_replace($this->sld . $asKey . $this->sld, $this->sld . $asKeyReplace . $this->sld . ' AS ' . $this->sld . $asKey . $this->sld, $columnsString) : str_replace($this->sld . $asKey . $this->sld, $this->sld . $asKey . $this->sld . ' AS ' . $this->sld . $asKeyReplace . $this->sld, $columnsString);
			}
		}
		return (string)$columnsString;
	}

	/**
	 * Отправка запроса
	 * @param string $first
	 * @return PDOStatement|bool
	 */
	private function sendQuery(string $first = ''): bool|PDOStatement {
		$query = $first . ' ';
		if (count($this->join) > 0) $query .= implode(' ', $this->join);
		if (count($this->where) > 0) {
			$endElement = array_pop($this->where);
			if ($endElement != 'AND' and $endElement != 'OR') $this->where[] = $endElement;
			$query .= ' WHERE ' . implode(' ', $this->where);
		}
		if (strlen($this->group) > 0) $query .= ' ' . $this->group;
		if (strlen($this->having) > 0) $query .= ' ' . $this->having;
		if (strlen($this->order) > 0) $query .= ' ' . $this->order;
		if (strlen($this->limit) > 0) $query .= ' ' . $this->limit;

		$result = $this->pdo->sendQuery($query, $this->data, $this->backtraceFile, $this->backtraceLine, $this->debug);
		$this->data = [];
		$this->where = [];
		$this->join = [];
		$this->group = '';
		$this->order = '';
		$this->limit = '';
		$this->having = '';
		return $result;
	}
}