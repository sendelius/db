<?php

namespace Sendelius\Db;

class Migrate {
	/**
	 * Хранилище экземпляра класса SPDO
	 * @var SPDO
	 */
	public SPDO $pdo;

	/**
	 * Описание структур таблиц
	 * @var array
	 */
	public array $tablesSchema = [];

	/**
	 * Запуск процесса миграции
	 * @param bool $safeMode
	 * @return void
	 */
	public function start(bool $safeMode = true): void {
		foreach ($this->tablesSchema as $tableName => $schema) {
			if (!$this->tableExists($tableName)) {
				$this->createTable($tableName, $schema);
			} else {
				$this->updateTable($tableName, $schema, $safeMode);
			}
		}
	}

	/**
	 * Обновление таблицы
	 * @param string $tableName
	 * @param array $desired
	 * @param bool $safeMode
	 * @return void
	 */
	private function updateTable(string $tableName, array $desired, bool $safeMode): void {
		$changes = [];

		// колонки
		$currentCols = $this->getCurrentSchema($tableName);
		$changes = array_merge($changes, $this->compareSchemas($currentCols, $desired, $safeMode));

		// индексы
		$currentIdx = $this->getCurrentIndexes($tableName);
		$changes = array_merge($changes, $this->compareIndexes($currentIdx, $desired, $safeMode));

		if (!empty($changes)) {
			$sql = "ALTER TABLE `{$tableName}` " . implode(", ", $changes);
			$this->pdo->exec($sql);
		}
	}

	/**
	 * Создание таблицы
	 * @param string $tableName
	 * @param array $schema
	 * @return void
	 */
	private function createTable(string $tableName, array $schema): void {
		$cols = [];
		$indexes = [];

		foreach ($schema as $name => $def) {
			if (!is_array($def)) continue;
			$cols[] = $this->buildColumnSql($name, $def);

			// индексы при создании
			if (!empty($def['index'])) {
				$indexes[] = "INDEX `idx_{$name}` (`{$name}`)";
			}
			if (!empty($def['unique'])) {
				$indexes[] = "UNIQUE `uniq_{$name}` (`{$name}`)";
			}
		}

		$sql = "CREATE TABLE `{$tableName}` (" . implode(", ", array_merge($cols, $indexes)) . ")";
		$this->pdo->exec($sql);
	}

	/**
	 * Проверка наличия таблицы в базе
	 * @param string $tableName
	 * @return bool
	 */
	private function tableExists(string $tableName): bool {
		$stmt = $this->pdo->prepare("SHOW TABLES LIKE ?");
		$stmt->execute([$tableName]);
		return (bool)$stmt->fetchColumn();
	}

	/**
	 * Получение текущей схемы
	 * @param string $tableName
	 * @return array
	 */
	private function getCurrentSchema(string $tableName): array {
		$columns = [];
		$result = $this->pdo->query("SHOW COLUMNS FROM `{$tableName}`");
		foreach ($result as $col) {
			$columns[$col['Field']] = $col;
		}
		return $columns;
	}

	/**
	 * Получение текущих индексов
	 * @param string $tableName
	 * @return array
	 */
	private function getCurrentIndexes(string $tableName): array {
		$indexes = [];
		$res = $this->pdo->query("SHOW INDEXES FROM `{$tableName}`");
		foreach ($res as $row) {
			$indexes[$row['Key_name']][] = $row['Column_name'];
		}
		return $indexes;
	}

	/**
	 * Сравнение схем
	 * @param array $current
	 * @param array $desired
	 * @param bool $safeMode
	 * @return array
	 */
	private function compareSchemas(array $current, array $desired, bool $safeMode): array {
		$changes = [];
		foreach ($desired as $field => $def) {
			if (!is_array($def)) continue; // пропуск служебных записей
			if (!isset($current[$field])) {
				$changes[] = "ADD COLUMN " . $this->buildColumnSql($field, $def);
			} else {
				if (!$def['auto_increment'] && !$def['primary'] && !$this->compareColumnType($current[$field], $def)) {
					$changes[] = "MODIFY COLUMN " . $this->buildColumnSql($field, $def);
				}
			}
		}
		if (!$safeMode) {
			foreach ($current as $field => $_) {
				if (!isset($desired[$field])) {
					$changes[] = "DROP COLUMN `{$field}`";
				}
			}
		}
		return $changes;
	}

	/**
	 * Сравнение индексов
	 * @param array $currentIdx
	 * @param array $desiredSchema
	 * @param bool $safeMode
	 * @return array
	 */
	private function compareIndexes(array $currentIdx, array $desiredSchema, bool $safeMode): array {
		$changes = [];

		foreach ($desiredSchema as $field => $def) {
			if (!is_array($def)) continue;

			// обычный индекс
			if (!empty($def['index'])) {
				$idxName = "idx_{$field}";
				if (!isset($currentIdx[$idxName])) {
					$changes[] = "ADD INDEX `{$idxName}` (`{$field}`)";
				}
			}

			// уникальный индекс
			if (!empty($def['unique'])) {
				$uniqName = "uniq_{$field}";
				if (!isset($currentIdx[$uniqName])) {
					$changes[] = "ADD UNIQUE `{$uniqName}` (`{$field}`)";
				}
			}
		}

		// удаление лишних индексов
		if (!$safeMode) {
			foreach ($currentIdx as $idxName => $cols) {
				// пропускаем PRIMARY
				if ($idxName === 'PRIMARY') continue;

				$existsInDesired = false;
				foreach ($desiredSchema as $field => $def) {
					if (!is_array($def)) continue;
					if ("idx_{$field}" === $idxName && !empty($def['index'])) {
						$existsInDesired = true;
					}
					if ("uniq_{$field}" === $idxName && !empty($def['unique'])) {
						$existsInDesired = true;
					}
				}
				if (!$existsInDesired) {
					$changes[] = "DROP INDEX `{$idxName}`";
				}
			}
		}

		return $changes;
	}

	/**
	 * Сравнение типов колонок
	 * @param array $currentCol
	 * @param array $desiredDef
	 * @return bool
	 */
	private function compareColumnType(array $currentCol, array $desiredDef): bool {
		$type = strtolower($desiredDef['type']);
		if ($type == 'boolean') $type = 'tinyint(1)';
		elseif (!empty($desiredDef['length'])) {
			$type .= '(' . $desiredDef['length'] . ')';
		}
		if ($desiredDef['unsigned']) {
			$type .= ' unsigned';
		}
		$currentType = strtolower($currentCol['Type']);
		return $type === $currentType;
	}

	/**
	 * Подготовка sql запроса
	 * @param string $name
	 * @param array $def
	 * @return string
	 */
	private function buildColumnSql(string $name, array $def): string {
		$type = strtoupper($def['type']);
		if ($type == 'BOOLEAN') {
			$type = 'TINYINT';
			$def['length'] = 1;
		}
		if (!empty($def['length'])) {
			$type .= "({$def['length']})";
		}
		$sql = "`{$name}` {$type}";
		if ($def['unsigned']) $sql .= " UNSIGNED";
		if ($def['auto_increment']) $sql .= " AUTO_INCREMENT";
		if (!empty($def['default'])) {
			$sql .= " DEFAULT " . (strtoupper($def['default']) === 'CURRENT_TIMESTAMP' ? "CURRENT_TIMESTAMP" : "'{$def['default']}'");
		}
		if (!empty($def['on_update'])) {
			$sql .= " ON UPDATE {$def['on_update']}";
		}
		if ($def['primary']) $sql .= " PRIMARY KEY";
		return $sql;
	}
}