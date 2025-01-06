<?php

namespace vkleo\medoo;

use Exception;
use Medoo\Medoo;
use support\Config;

class MedooDb
{
    /**
     * 单例实例
     * @var self|null
     */
    private static ?self $instance = null;

    /**
     * Medoo 数据库实例
     * @var Medoo
     */
    protected Medoo $database;

    /**
     * 当前查询的数据结构
     * @var array
     */
    protected array $queryData = [];

    /**
     * 禁止外部实例化，确保单例模式
     */
    private function __construct()
    {
        try {
            $this->initializeDatabase();
        } catch (Exception $e) {
            throw new Exception($e->getMessage());
        }
    }

    /**
     * 禁止克隆，确保单例模式
     */
    private function __clone()
    {
    }

    /**
     * 禁止反序列化，确保单例模式
     */
    private function __wakeup()
    {
    }

    /**
     * 初始化数据库连接，从配置文件加载数据库设置
     *
     * @throws \Exception 如果找不到数据库配置
     */
    private function initializeDatabase(): void
    {
        $dbConfig = Config::get('database');
        if (empty($dbConfig)) {
            throw new \Exception("找不到数据库配置，请检查config/database.php是否正确。");
        }
        $this->database = new Medoo($dbConfig);
    }

    /**
     * 获取单例实例，用于链式调用
     *
     * @return self 返回 MedooChainQuery 的单例实例
     */
    public static function getInstance(): self
    {
        if (null === static::$instance) {
            static::$instance = new static();
        }
        return static::$instance;
    }

    /**
     * 静态方法开始链式调用，选择要查询的列
     *
     * @param array|string $columns 查询的列名或'*'表示所有列
     * @return self 返回当前实例以支持链式调用
     */
    public static function select(array|string $columns = '*'): self
    {
        return static::getInstance()->doSelect($columns);
    }

    /**
     * 内部方法用于实际执行 select 操作
     *
     * @param array|string $columns 查询的列名或'*'表示所有列
     * @return self 返回当前实例以支持链式调用
     */
    private function doSelect(array|string $columns = '*'): self
    {
        $this->queryData['SELECT'] = $columns;
        return $this;
    }

    /**
     * 设置查询表名
     *
     * @param string $table 表名
     * @return self 返回当前实例以支持链式调用
     */
    public function from(string $table): self
    {
        $this->queryData['FROM'] = $table;
        return $this;
    }

    /**
     * 添加 AND WHERE 条件到查询
     *
     * @param string|array $column 列名或者条件数组
     * @param string $operator 比较操作符，默认是 '='
     * @param mixed $value 比较值
     * @return self 返回当前实例以支持链式调用
     */
    public function where(string|array $column, string $operator = '=', mixed $value = null): self
    {
        if ($value === null && is_array($column)) {
            return $this->whereCondition($column, 'AND');
        }

        if (is_string($operator) && in_array(strtoupper($operator), ['IN', 'NOT IN', 'ON', 'NOT ON'])) {
            return $this->handleSpecialConditions($column, $operator, $value, 'AND');
        }

        if (is_array($value)) {
            return $this->handleSpecialConditions($column, 'IN', $value, 'AND');
        }

        return $this->whereCondition([$column => [$operator, $value]], 'AND');
    }

    /**
     * 添加 OR WHERE 条件到查询
     *
     * @param string|array $column 列名或者条件数组
     * @param string $operator 比较操作符，默认是 '='
     * @param mixed $value 比较值
     * @return self 返回当前实例以支持链式调用
     */
    public function orWhere(string|array $column, string $operator = '=', mixed $value = null): self
    {
        if ($value === null && is_array($column)) {
            return $this->whereCondition($column, 'OR');
        }

        if (is_string($operator) && in_array(strtoupper($operator), ['IN', 'NOT IN', 'ON', 'NOT ON'])) {
            return $this->handleSpecialConditions($column, $operator, $value, 'OR');
        }

        if (is_array($value)) {
            return $this->handleSpecialConditions($column, 'IN', $value, 'OR');
        }

        return $this->whereCondition([$column => [$operator, $value]], 'OR');
    }

    /**
     * 构建特殊条件（IN, NOT IN, ON, NOT ON）
     *
     * @param string $column 列名
     * @param string $operator 特殊操作符
     * @param mixed $value 比较值
     * @param string $logicalOperator 逻辑操作符（AND 或 OR）
     * @return self 返回当前实例以支持链式调用
     * @throws \InvalidArgumentException 如果提供的值不符合预期类型
     */
    private function handleSpecialConditions(string $column, string $operator, mixed $value, string $logicalOperator): self
    {
        $operator = strtoupper($operator);

        if (in_array($operator, ['ON', 'NOT ON'])) {
            if (!is_array($value)) {
                throw new \InvalidArgumentException('Value for ON and NOT ON operators must be an associative array.');
            }
            $this->addCondition([$column => $value], $logicalOperator); // 直接传递给 Medoo
        } else {
            if (!is_array($value)) {
                throw new \InvalidArgumentException('Value for IN and NOT IN operators must be an array of values.');
            }
            $this->addCondition([$column => [$operator, $value]], $logicalOperator);
        }

        return $this;
    }

    /**
     * 添加条件到 WHERE 子句中
     *
     * @param array $data 条件数据
     * @param string $operator 逻辑操作符（AND 或 OR）
     */
    private function addCondition(array $data, string $operator): void
    {
        if (isset($this->queryData['WHERE']) && is_array(end($this->queryData['WHERE']))) {
            end($this->queryData['WHERE']);
            $lastKey = key($this->queryData['WHERE']);
            if (is_string($lastKey) && strtoupper($lastKey) === $operator) {
                $this->queryData['WHERE'][$lastKey][] = $data;
                return;
            }
        }
        $this->queryData['WHERE'][] = $data;
        if ($operator !== 'AND') { // 默认为 AND
            $this->queryData['WHERE'][] = strtoupper($operator);
        }
    }

    /**
     * 构建条件并添加到查询数据中
     *
     * @param array $conditions 条件数组
     * @param string $operator 逻辑操作符（AND 或 OR）
     * @return self 返回当前实例以支持链式调用
     */
    private function whereCondition(array $conditions, string $operator = 'AND'): self
    {
        foreach ($conditions as $column => $condition) {
            if (is_array($condition)) {
                list($op, $value) = $condition;
                $this->addCondition([$column => [$op, $value]], $operator);
            } else {
                $this->addCondition([$column => $condition], $operator);
            }
        }
        return $this;
    }

    /**
     * 添加 LIKE 条件到查询中。
     *
     * @param string $column 数据库列名。
     * @param string $value 匹配模式，可以包含通配符。
     * @param string|null $position 指定通配符的位置，可选值：'left', 'right', 'both'（默认）。
     * @return static 返回当前实例以支持链式调用。
     */
    public function like(string $column, string $value, ?string $position = 'both'): static
    {
        return $this->likeCondition($column, $value, $position, 'AND');
    }

    /**
     * 添加 NOT LIKE 条件到查询中。
     *
     * @param string $column 数据库列名。
     * @param string $value 匹配模式，可以包含通配符。
     * @param string|null $position 指定通配符的位置，可选值：'left', 'right', 'both'（默认）。
     * @return static 返回当前实例以支持链式调用。
     */
    public function notLike(string $column, string $value, ?string $position = 'both'): static
    {
        return $this->likeCondition($column, $value, $position, 'AND', true);
    }

    /**
     * 添加 OR LIKE 条件到查询中。
     *
     * @param string $column 数据库列名。
     * @param string $value 匹配模式，可以包含通配符。
     * @param string|null $position 指定通配符的位置，可选值：'left', 'right', 'both'（默认）。
     * @return static 返回当前实例以支持链式调用。
     */
    public function orLike(string $column, string $value, ?string $position = 'both'): static
    {
        return $this->likeCondition($column, $value, $position, 'OR');
    }

    /**
     * 添加 OR NOT LIKE 条件到查询中。
     *
     * @param string $column 数据库列名。
     * @param string $value 匹配模式，可以包含通配符。
     * @param string|null $position 指定通配符的位置，可选值：'left', 'right', 'both'（默认）。
     * @return static 返回当前实例以支持链式调用。
     */
    public function orNotLike(string $column, string $value, ?string $position = 'both'): static
    {
        return $this->likeCondition($column, $value, $position, 'OR', true);
    }

    /**
     * 内部方法用于构建 LIKE 或 NOT LIKE 查询条件。
     *
     * @param string $column 数据库列名。
     * @param string $value 匹配模式，可以包含通配符。
     * @param string|null $position 指定通配符的位置，可选值：'left', 'right', 'both'（默认）。
     * @param string $operator 逻辑操作符，AND 或 OR。
     * @param bool $not 是否使用 NOT LIKE，默认是 false。
     * @return static 返回当前实例以支持链式调用。
     */
    protected function likeCondition(string $column, string $value, ?string $position, string $operator, bool $not = false): static
    {
        // 根据位置参数构建通配符表达式
        $wildcard = match ($position) {
            'left' => "%{$value}",
            'right' => "{$value}%",
            default => "%{$value}%",
        };

        // 构建 LIKE 或 NOT LIKE 操作符
        $op = $not ? 'NOT LIKE' : 'LIKE';

        // 将条件添加到查询数据中
        $this->whereCondition([$column => [$op, $wildcard]], $operator);

        return $this;
    }

    /**
     * 设置查询结果排序规则
     *
     * @param string $column 排序的列名
     * @param string $direction 排序方向，ASC 或 DESC，默认是 ASC
     * @return self 返回当前实例以支持链式调用
     */
    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        $this->queryData['ORDER'] = [$column => $direction];
        return $this;
    }

    /**
     * 设置查询结果的数量限制
     *
     * @param int $limit 限制的数量
     * @param int $offset 偏移量，默认是 0
     * @return self 返回当前实例以支持链式调用
     */
    public function limit(int $limit, int $offset = 0): self
    {
        $this->queryData['LIMIT'] = [$offset, $limit];
        return $this;
    }

    /**
     * 执行查询并返回结果
     *
     * @return array 查询结果集
     */
    public function get(): array
    {
        return $this->database->select(
            $this->queryData['FROM'],
            $this->queryData['SELECT'] ?? '*',
            array_merge_recursive([], ...array_filter([
                isset($this->queryData['WHERE']) ? ['WHERE' => $this->queryData['WHERE']] : null,
                isset($this->queryData['ORDER']) ? ['ORDER' => $this->queryData['ORDER']] : null,
                isset($this->queryData['LIMIT']) ? ['LIMIT' => $this->queryData['LIMIT']] : null,
            ]))
        );
    }

    /**
     * 清空当前查询条件，以便开始新的查询
     *
     * @return self 返回当前实例以支持链式调用
     */
    public function clear(): self
    {
        $this->queryData = [];
        return $this;
    }

    /**
     * 插入单条或多条记录到指定表中。
     *
     * @param string $table 表名。
     * @param array $data 数据数组。如果是一个关联数组，则插入单条记录；如果是索引数组，则批量插入多条记录。
     * @return int|bool 成功插入的记录数或布尔值表示操作是否成功。
     * @throws InvalidArgumentException 如果数据为空或格式不正确。
     */
    public function insert(string $table, array $data): int|bool {
        if (empty($data)) {
            throw new InvalidArgumentException('Data cannot be empty.');
        }

        // 检查 $data 是否为关联数组（即单条记录），通过比较键与连续整数范围来判断
        if (array_keys($data) !== range(0, count($data) - 1)) {
            // 是关联数组，进行单条记录插入
            return $this->database->insert($table, $data);
        }

        // 否则，$data 应该是一个包含多个关联数组的索引数组，用于批量插入
        foreach ($data as $record) {
            if (!is_array($record) || array_keys($record) !== array_keys($record)) {
                throw new InvalidArgumentException('Each record must be an associative array.');
            }
        }

        // 批量插入多条记录
        return $this->database->insert($table, $data);
    }

    /**
     * 删除符合条件的记录。
     *
     * @param string $table 表名。
     * @return int 删除的记录数。
     * @throws InvalidArgumentException 如果没有 WHERE 条件，防止意外删除所有数据。
     */
    public function delete(string $table): int {
        if (empty($this->queryData['WHERE'])) {
            throw new InvalidArgumentException('DELETE operation requires a WHERE clause to prevent accidental data loss.');
        }

        // 使用已构建的 WHERE 条件来执行删除操作
        return $this->database->delete($table, [
            'WHERE' => $this->queryData['WHERE']
        ]);
    }

    /**
     * 更新单条或多条记录。
     *
     * @param string $table 表名。
     * @param array $data 要更新的数据。如果是一个关联数组，则更新单条记录；如果是索引数组，则批量更新多条记录。
     * @param string|null $primaryKey 主键字段名称，当批量更新时需要提供。
     * @return int 受影响的行数。
     * @throws InvalidArgumentException 如果数据为空、缺少 WHERE 条件且未提供主键，或者批量更新时记录中缺少主键字段。
     */
    public function update(string $table, array $data, ?string $primaryKey = null): int {
        if (empty($data)) {
            throw new InvalidArgumentException('Data cannot be empty.');
        }

        // 确保有 WHERE 条件或提供了主键字段名，以避免无条件更新
        if (empty($this->queryData['WHERE']) && $primaryKey === null) {
            throw new InvalidArgumentException('UPDATE operation requires either a WHERE clause or a primary key field name.');
        }

        // 判断是单条更新还是批量更新
        if (array_keys($data) !== range(0, count($data) - 1)) {
            // 单条更新
            return $this->database->update($table, $data, [
                'WHERE' => $this->queryData['WHERE'] ?? []
            ]);
        }

        // 批量更新
        $affectedRows = 0;
        foreach ($data as $record) {
            if (!is_array($record) || !isset($record[$primaryKey])) {
                throw new InvalidArgumentException('Each record for batch update must contain the primary key field.');
            }

            // 获取主键值并从记录中移除它，然后用剩余的数据进行更新
            $pkValue = $record[$primaryKey];
            unset($record[$primaryKey]);

            // 执行更新操作，并累加受影响的行数
            $affectedRows += $this->database->update($table, $record, [
                $primaryKey => $pkValue
            ]);
        }

        return $affectedRows;
    }
}