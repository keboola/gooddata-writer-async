<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-09-09
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;
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

	protected $cache = array();


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
	protected function fetchTableRows($tableId, $whereColumn = null, $whereValue = null, $options = array(), $cache = true)
	{
		$exportOptions = array();
		if ($whereColumn) {
			$exportOptions = array_merge($exportOptions, array(
				'whereColumn' => $whereColumn,
				'whereValues' => !is_array($whereValue) ? array($whereValue) : $whereValue
			));
		}
		if (count($options)) {
			$exportOptions = array_merge($exportOptions, $options);
		}
		try {
			return $this->sapi_exportTable($tableId, $exportOptions, $cache);
		} catch (\Keboola\StorageApi\ClientException $e) {
			return array();
		}
	}

	/**
	 * Delete rows from SAPI table
	 */
	protected function deleteTableRows($tableId, $whereColumn, $whereValues)
	{
		$options = array(
			'whereColumn' => $whereColumn,
			'whereValues' => is_array($whereValues)? $whereValues : array($whereValues)
		);
		try {
			$this->storageApiClient->deleteTableRows($tableId, $options);
		} catch (ClientException $e) {
			// Ignore if table does not exist
			if (!in_array($e->getCode(), array(400, 404))) {
				throw $e;
			}
		}
	}

	/**
	 * Update SAPI table row
	 */
	protected function updateTableRow($tableId, $primaryKey, $data, $indices = array())
	{
		return $this->updateTable($tableId, $primaryKey, array_keys($data), array($data), $indices);
	}

	/**
	 * Try to create table, ignore if already exists
	 */
	protected function createTable($tableId, $primaryKey, $headers, $indices = array())
	{
		try {
			$this->saveTable($tableId, $primaryKey, $headers, array(), false, false, $indices);
		} catch (ClientException $e) {
			if (!in_array($e->getCode(), array(400, 404))) {
				throw $e;
			}
		}
	}

	/**
	 * Update SAPI table
	 */
	protected function updateTable($tableId, $primaryKey, $headers, $data = array(), $indices = array())
	{
		return $this->saveTable($tableId, $primaryKey, $headers, $data, true, true, $indices);
	}

	/**
	 * Save SAPI table
	 */
	protected function saveTable($tableId, $primaryKey, $headers, $data = array(), $incremental = true, $partial = true, $indices = array())
	{
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
	}


	/**
	 * Create configuration table
	 */
	protected function createConfigTable($tableName)
	{
		$tableId = $this->bucketId . '.' . $tableName;

		if (!isset($this->tables[$tableName]) || $this->storageApiClient->tableExists($tableId))
			return false;

		$this->createTable(
			$tableId,
			$this->tables[$tableName]['primaryKey'],
			$this->tables[$tableName]['columns'],
			$this->tables[$tableName]['indices']);
	}

	/**
	 * Empty configuration table
	 */
	public function resetConfigTable($tableName)
	{
		if (!isset($this->tables[$tableName])) return false;

		$this->saveTable($this->bucketId . '.' . $tableName,
			$this->tables[$tableName]['primaryKey'], $this->tables[$tableName]['columns'], array(), false, false,
			$this->tables[$tableName]['indices']);
	}

	/**
	 * Update configuration table
	 * $data must contain column names in keys if not all
	 */
	protected function updateConfigTable($tableName, $data, $incremental = true)
	{
		if (!isset($this->tables[$tableName])) return false;

		$firstRow = current($data);
		$result = $this->saveTable(
			$this->bucketId . '.' . $tableName,
			$this->tables[$tableName]['primaryKey'],
			(count($firstRow) == count($this->tables[$tableName]['columns']))? $this->tables[$tableName]['columns'] : array_keys($firstRow),
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
		if (!isset($this->tables[$tableName])) return false;

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
	protected function getConfigTableRow($tableName, $id, $cache=true)
	{
		if (!isset($this->tables[$tableName])) return false;

		$result = $this->fetchTableRows($this->bucketId . '.' . $tableName, $this->tables[$tableName]['primaryKey'], $id, array(), $cache);
		return count($result) ? current($result) : false;
	}

	/**
	 * Check if configuration table contains all required columns
	 */
	protected function checkConfigTable($tableName, $columns)
	{
		if (!isset($this->tables[$tableName])) return false;

		//@TODO Remove sometimes in october ;o)
		// MIGRATE FILTERS CONFIGURATION
		if (($tableName == 'filters_projects' && count($columns) == 2)
			|| ($tableName == 'filters_users' && count($columns) == 2)
			|| ($tableName == 'filters' && in_array('uri', $columns))) {
			try {
				$oldFilters = $this->fetchTableRows($this->bucketId . '.filters');
				$oldFiltersUsers = $this->fetchTableRows($this->bucketId . '.filters_users');

				$this->storageApiClient->dropTable($this->bucketId . '.filters');
				$this->storageApiClient->dropTable($this->bucketId . '.filters_projects');
				$this->storageApiClient->dropTable($this->bucketId . '.filters_users');

				$filtersData = array();
				$filtersProjectsData = array();
				$filtersUsersData = array();

				$filtersTable = new StorageApiTable($this->storageApiClient, $this->bucketId . '.filters', null, 'name');
				$filtersTable->setHeader(array('name', 'attribute', 'operator', 'value'));

				$filtersProjectsTable = new StorageApiTable($this->storageApiClient, $this->bucketId . '.filters_projects', null, 'uri');
				$filtersProjectsTable->setHeader(array('uri', 'filter', 'pid'));
				$filtersProjectsTable->setIndices(array('filter', 'pid'));

				foreach ($oldFilters as $f) {
					preg_match('/^\/gdc\/md\/([a-z0-9]+)\/obj\/([0-9]+)$/', $f['uri'], $uri);
					if (count($uri) == 3) {
						$filtersData[] = array($f['name'], $f['attribute'], $f['operator'], $f['element']);
						$filtersProjectsData[] = array($f['uri'], $f['name'], $uri[1]);
					} else {
						throw new WrongConfigurationException('Filters configuration could not be migrated. Filters uri ' . $f['uri'] . ' does not seem valid');
					}
				}

				$filtersProjectsTable->setFromArray($filtersProjectsData);
				$filtersProjectsTable->save();
				$filtersTable->setFromArray($filtersData);
				$filtersTable->save();


				$filtersUsersTable = new StorageApiTable($this->storageApiClient, $this->bucketId . '.filters_users', null, 'id');
				$filtersUsersTable->setHeader(array('id', 'filter', 'email'));
				$filtersUsersTable->setIndices(array('filter', 'email'));
				foreach ($oldFiltersUsers as $fu) {
					$filtersUsersData[] = array(sha1($fu['filterName'] . '.' . $fu['userEmail']), $fu['filterName'], $fu['userEmail']);
				}
				$filtersUsersTable->setFromArray($filtersUsersData);
				$filtersUsersTable->save();

			} catch (\Exception $e) {
				// ignore - race condition
			}

			return true;
		}
		if ($tableName == 'filters' && !in_array('over', $columns)) {
			try {
				$this->storageApiClient->addTableColumn($this->bucketId . '.filters', 'over');
			} catch (\Exception $e) {
				// ignore - race condition
			}
			$columns[] = 'over';
		}
		if ($tableName == 'filters' && !in_array('to', $columns)) {
			try {
				$this->storageApiClient->addTableColumn($this->bucketId . '.filters', 'to');
			} catch (\Exception $e) {
				// ignore - race condition
			}
			$columns[] = 'to';
		}
		//@TODO Remove sometimes in october ;o)


		// Allow tables to have more columns then according to definitions
		if (count(array_diff($this->tables[$tableName]['columns'], $columns))) {
			throw new WrongConfigurationException(sprintf("Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
				$tableName, implode(',', $columns), implode(',', $this->tables[$tableName]['columns'])));
		}

		return true;
	}

	/**
	 * Get all rows from configuration table
	 */
	protected function getConfigTable($tableName)
	{
		if (!isset($this->tables[$tableName])) return false;

		try {
			$table = $this->fetchTableRows($this->bucketId . '.' . $tableName);
		} catch (ClientException $e) {
			if ($e->getCode() == 404) {
				$this->createConfigTable($tableName);
				$table = array();
			} else {
				throw $e;
			}
		}

		if (count($table)) {
			$this->checkConfigTable($tableName, array_keys(current($table)));
		}

		return $table;
	}


	public function bucketAttributes()
	{
		$bucketData = null;
		foreach ($this->sapi_listBuckets() as $bucket) {
			if ($this->bucketId == $bucket['id']) {
				$bucketData = $bucket;
			}
		}
		if (!$bucketData) {
			return false;
		}

		return $this->parseAttributes($bucketData['attributes']);
	}

	protected function parseAttributes($attributes)
	{
		$result = array();
		foreach ($attributes as $attr) {
			$attrArray = explode('.', $attr['name']);
			if (count($attrArray) > 1) {
				if (!isset($result[$attrArray[0]])) {
					$result[$attrArray[0]] = array();
				}
				$result[$attrArray[0]][$attrArray[1]] = $attr['value'];
			} else {
				$result[$attr['name']] = $attr['value'];
			}
		}
		return $result;
	}



	/********************
	 ********************
	 * @section SAPI cache
	 * @TODO TEMPORAL SOLUTION - move somewhere else (maybe create subclass of SAPI client)
	 ********************/


	public function clearCache()
	{
		$this->cache = array();
	}

	public function sapi_listBuckets()
	{
		$cacheKey = 'listBuckets';
		if (!isset($this->cache[$cacheKey])) {
			$this->cache[$cacheKey] = $this->storageApiClient->listBuckets();
		}
		return $this->cache[$cacheKey];
	}

	public function sapi_bucketExists($bucketId)
	{
		foreach ($this->sapi_listBuckets() as $bucket) {
			if ($bucketId == $bucket['id']) {
				return true;
			}
		}
		return false;
	}

	public function sapi_listTables($bucketId = null)
	{
		$cacheKey = 'listTables.' . $bucketId;
		if (!isset($this->cache[$cacheKey])) {
			$this->cache[$cacheKey] = $this->storageApiClient->listTables($bucketId, array('include' => ''));
		}
		return $this->cache[$cacheKey];
	}

	public function sapi_tableExists($tableId)
	{
		foreach ($this->sapi_listTables() as $table) {
			if ($tableId == $table['id']) {
				return true;
			}
		}
		return false;
	}

	public function sapi_getTable($tableId)
	{
		$cacheKey = 'getTable.' . $tableId;
		if (!isset($this->cache[$cacheKey])) {
			$this->cache[$cacheKey] = $this->storageApiClient->getTable($tableId);
		}
		return $this->cache[$cacheKey];
	}


	public function sapi_exportTable($tableId, $options = array(), $cache = true)
	{
		$cacheKey = 'exportTable.' . $tableId;
		if (count($options)) {
			$keyOptions = $options;
			if (isset($keyOptions['whereValues']) && count($keyOptions['whereValues'])) {
				$keyOptions['whereValues'] = implode('.', $keyOptions['whereValues']);
			}
			$cacheKey .=  '.' . implode(':', array_keys($keyOptions)) . '.' . implode(':', array_values($keyOptions));
		}
		if (!isset($this->cache[$cacheKey]) || !$cache) {
			$csv = $this->storageApiClient->exportTable($tableId, null, $options);
			$this->cache[$cacheKey] = StorageApiClient::parseCsv($csv, true);
		}
		return $this->cache[$cacheKey];
	}
}