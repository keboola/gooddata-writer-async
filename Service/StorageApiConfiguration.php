<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-09-09
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\Table as StorageApiTable;
use Keboola\StorageApi\ClientException;

abstract class StorageApiConfiguration
{

    /**
     * @var StorageApiClient $storageApiClient
     */
    protected $storageApiClient;


    /**
     * Should contain list of configuration tables, eg:
     * array(
     *     self::PROJECT_USERS_TABLE_NAME => array(
     *         'columns' => array('id', 'pid', 'email', 'role', 'action'),
     *         'primaryKey' => 'id',
     *         'indices' => array('pid', 'email')
     *     )
     * )
     * @var array
     */
    protected $tables;

    protected $cache = [];


    /**
     * @var string
     */
    public $bucketId;


    public function __construct($storageApiClient)
    {
        $this->storageApiClient = $storageApiClient;
    }

    /**
     * Get selected rows from SAPI table
     */
    protected function fetchTableRows($tableId, $whereColumn = null, $whereValue = null, $options = [], $cache = true)
    {
        $exportOptions = [];
        if ($whereColumn) {
            $exportOptions = array_merge($exportOptions, [
                'whereColumn' => $whereColumn,
                'whereValues' => !is_array($whereValue) ? [$whereValue] : $whereValue
            ]);
        }
        if (count($options)) {
            $exportOptions = array_merge($exportOptions, $options);
        }
        try {
            return $this->sapiExportTable($tableId, $exportOptions, $cache);
        } catch (\Keboola\StorageApi\ClientException $e) {
            return [];
        }
    }

    /**
     * Delete rows from SAPI table
     */
    protected function deleteTableRows($tableId, $whereColumn, $whereValues)
    {
        $options = [
            'whereColumn' => $whereColumn,
            'whereValues' => is_array($whereValues) ? $whereValues : [$whereValues]
        ];
        try {
            $this->storageApiClient->deleteTableRows($tableId, $options);
        } catch (ClientException $e) {
            // Ignore if table does not exist
            if (!in_array($e->getCode(), [400, 404])) {
                if ($e->getCode() == 403) {
                    throw new WrongConfigurationException('Your token does not have access to table ' . $tableId);
                }
                throw $e;
            }
        }
    }

    /**
     * Update SAPI table row
     */
    protected function updateTableRow($tableId, $primaryKey, $data, $indices = [])
    {
        return $this->updateTable($tableId, $primaryKey, array_keys($data), [$data], $indices);
    }

    /**
     * Try to create table, ignore if already exists
     */
    protected function createTable($tableId, $primaryKey, $headers, $indices = [])
    {
        try {
            $this->saveTable($tableId, $primaryKey, $headers, [], false, false, $indices);
        } catch (ClientException $e) {
            if (!in_array($e->getCode(), [400, 404])) {
                throw $e;
            }
        }
    }

    /**
     * Update SAPI table
     */
    protected function updateTable($tableId, $primaryKey, $headers, $data = [], $indices = [])
    {
        return $this->saveTable($tableId, $primaryKey, $headers, $data, true, true, $indices);
    }

    /**
     * Save SAPI table
     */
    protected function saveTable(
        $tableId,
        $primaryKey,
        $headers,
        $data = [],
        $incremental = true,
        $partial = true,
        $indices = []
    ) {
        try {
            $table = new StorageApiTable($this->storageApiClient, $tableId, null, $primaryKey);
            $table->setHeader($headers);
            if (count($data)) {
                $table->setFromArray($data);
            }
            if (count($indices) && !$this->storageApiClient->tableExists($tableId)) {
                $table->setIndices($indices);
            }
            $table->setIncremental($incremental);
            $table->setPartial($partial);
            $table->save();
            return $table;
        } catch (\Keboola\StorageApi\ClientException $e) {
            if ($e->getCode() == 403) {
                throw new WrongConfigurationException('Your token does not have access to table ' . $tableId);
            }
            throw $e;
        }
    }


    /**
     * Create configuration table
     */
    protected function createConfigTable($tableName)
    {
        $tableId = $this->bucketId . '.' . $tableName;

        if (!isset($this->tables[$tableName]) || $this->storageApiClient->tableExists($tableId)) {
            return false;
        }

        return $this->createTable(
            $tableId,
            $this->tables[$tableName]['primaryKey'],
            $this->tables[$tableName]['columns'],
            $this->tables[$tableName]['indices']
        );
    }

    /**
     * Empty configuration table
     */
    public function resetConfigTable($tableName)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        return $this->saveTable(
            $this->bucketId . '.' . $tableName,
            $this->tables[$tableName]['primaryKey'],
            $this->tables[$tableName]['columns'],
            [],
            false,
            false,
            $this->tables[$tableName]['indices']
        );
    }

    /**
     * Update configuration table
     * $data must contain column names in keys if not all
     */
    protected function updateConfigTable($tableName, $data, $incremental = true)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        $firstRow = current($data);
        $result = $this->saveTable(
            $this->bucketId . '.' . $tableName,
            $this->tables[$tableName]['primaryKey'],
            (count($firstRow) == count($this->tables[$tableName]['columns'])) ? $this->tables[$tableName]['columns'] : array_keys($firstRow),
            $data,
            $incremental,
            true,
            $this->tables[$tableName]['indices']
        );

        $this->clearCache();
        return $result;
    }

    /**
     * Update row of configuration table
     */
    protected function updateConfigTableRow($tableName, $data)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        $result = $this->updateTableRow(
            $this->bucketId . '.' . $tableName,
            $this->tables[$tableName]['primaryKey'],
            $data,
            $this->tables[$tableName]['indices']
        );

        $this->clearCache();
        return $result;
    }

    /**
     * Get row from configuration table
     */
    protected function getConfigTableRow($tableName, $id, $cache = true)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        $result = $this->fetchTableRows(
            $this->bucketId . '.' . $tableName,
            $this->tables[$tableName]['primaryKey'],
            $id,
            [],
            $cache
        );
        return count($result) ? current($result) : false;
    }

    /**
     * Check if configuration table contains all required columns
     */
    protected function checkConfigTable($tableName, $columns)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        // Allow tables to have more columns then according to definitions
        if (count(array_diff($this->tables[$tableName]['columns'], $columns))) {
            throw new WrongConfigurationException(sprintf(
                "Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
                $tableName,
                implode(',', $columns),
                implode(',', $this->tables[$tableName]['columns'])
            ));
        }

        return true;
    }

    /**
     * Get all rows from configuration table
     */
    protected function getConfigTable($tableName)
    {
        if (!isset($this->tables[$tableName])) {
            return false;
        }

        try {
            $table = $this->fetchTableRows($this->bucketId . '.' . $tableName);
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                $this->createConfigTable($tableName);
                $table = [];
            } else {
                throw $e;
            }
        }

        if (count($table)) {
            $this->checkConfigTable($tableName, array_keys(current($table)));
        }

        return $table;
    }


    /********************
     ********************
     * @section SAPI cache
     * @TODO TEMPORAL SOLUTION - move somewhere else (maybe create subclass of SAPI client)
     ********************/


    public function clearCache()
    {
        $this->cache = [];
    }

    public function sapiListBuckets()
    {
        $cacheKey = 'listBuckets';
        if (!isset($this->cache[$cacheKey])) {
            $this->cache[$cacheKey] = $this->storageApiClient->listBuckets();
        }
        return $this->cache[$cacheKey];
    }

    public function sapiBucketExists($bucketId)
    {
        foreach ($this->sapiListBuckets() as $bucket) {
            if ($bucketId == $bucket['id']) {
                return true;
            }
        }
        return false;
    }

    public function sapiListTables($bucketId = null)
    {
        $cacheKey = 'listTables.' . $bucketId;
        if (!isset($this->cache[$cacheKey])) {
            $this->cache[$cacheKey] = $this->storageApiClient->listTables($bucketId, ['include' => '']);
        }
        return $this->cache[$cacheKey];
    }

    public function sapiTableExists($tableId)
    {
        foreach ($this->sapiListTables() as $table) {
            if ($tableId == $table['id']) {
                return true;
            }
        }
        return false;
    }

    public function sapiGetTable($tableId)
    {
        $cacheKey = 'getTable.' . $tableId;
        if (!isset($this->cache[$cacheKey])) {
            try {
                $this->cache[$cacheKey] = $this->storageApiClient->getTable($tableId);
            } catch (\Keboola\StorageApi\ClientException $e) {
                if ($e->getCode() == 403) {
                    throw new WrongConfigurationException('Your token does not have access to table ' . $tableId);
                }
                throw $e;
            }
        }
        return $this->cache[$cacheKey];
    }


    public function sapiExportTable($tableId, $options = [], $cache = true)
    {
        $cacheKey = 'exportTable.' . $tableId;
        if (count($options)) {
            $keyOptions = $options;
            if (isset($keyOptions['whereValues']) && count($keyOptions['whereValues'])) {
                $keyOptions['whereValues'] = implode('.', $keyOptions['whereValues']);
            }
            $cacheKey .= '.' . implode(':', array_keys($keyOptions)) . '.' . implode(':', array_values($keyOptions));
        }
        if (!isset($this->cache[$cacheKey]) || !$cache) {
            try {
                $csv = $this->storageApiClient->exportTable($tableId, null, $options);
            } catch (\Keboola\StorageApi\ClientException $e) {
                if ($e->getCode() == 403) {
                    throw new WrongConfigurationException('Your token does not have access to table ' . $tableId);
                }
                throw $e;
            }
            $this->cache[$cacheKey] = StorageApiClient::parseCsv($csv, true);
        }
        return $this->cache[$cacheKey];
    }
}
