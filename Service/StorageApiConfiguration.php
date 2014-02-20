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
	 * @var StorageApiClient $_storageApiClient
	 */
	protected $_storageApiClient;


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

	protected $_cache = array();


	/**
	 * @var string
	 */
	public $bucketId;
	
	
	public function __construct($storageApiClient)
	{
		$this->_storageApiClient = $storageApiClient;
	}

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 * @param array $options
	 * @return array
	 */
	protected function _fetchTableRows($tableId, $whereColumn = null, $whereValue = null, $options = array(), $cache = true)
	{
		$exportOptions = array();
		if ($whereColumn) {
			$exportOptions = array_merge($exportOptions, array(
				'whereColumn' => $whereColumn,
				'whereValues' => array($whereValue)
			));
		}
		if (count($options)) {
			$exportOptions = array_merge($exportOptions, $options);
		}
		return $this->sapi_exportTable($tableId, $exportOptions, $cache);
	}

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 */
	protected function _deleteTableRow($tableId, $whereColumn, $whereValue)
	{
		$this->_deleteTableRows($tableId, $whereColumn, array($whereValue));
	}

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param array $whereValues
	 */
	protected function _deleteTableRows($tableId, $whereColumn, $whereValues)
	{
		$options = array(
			'whereColumn' => $whereColumn,
			'whereValues' => $whereValues
		);
		$this->_storageApiClient->deleteTableRows($tableId, $options);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $data
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _updateTableRow($tableId, $primaryKey, $data, $indices = array())
	{
		return $this->_updateTable($tableId, $primaryKey, array_keys($data), array($data), $indices);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _createTable($tableId, $primaryKey, $headers, $indices = array())
	{
		return $this->_saveTable($tableId, $primaryKey, $headers, array(), false, false, $indices);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param array $data
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _updateTable($tableId, $primaryKey, $headers, $data = array(), $indices = array())
	{
		return $this->_saveTable($tableId, $primaryKey, $headers, $data, true, true, $indices);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param array $data
	 * @param bool $incremental
	 * @param bool $partial
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _saveTable($tableId, $primaryKey, $headers, $data = array(), $incremental = true, $partial = true, $indices = array())
	{
		$table = new StorageApiTable($this->_storageApiClient, $tableId, null, $primaryKey);
		$table->setHeader($headers);
		if (count($data)) {
			$table->setFromArray($data);
		}
		if (count($indices) && !$this->_storageApiClient->tableExists($tableId)) {
			$table->setIndices($indices);
		}
		$table->setIncremental($incremental);
		$table->setPartial($partial);
		$table->save();
		return $table;
	}


	protected function _createConfigTable($tableName)
	{
		$tableId = $this->bucketId . '.' . $tableName;

		if (!isset($this->tables[$tableName]) || $this->_storageApiClient->tableExists($tableId))
			return false;

		return $this->_createTable(
			$tableId,
			$this->tables[$tableName]['primaryKey'],
			$this->tables[$tableName]['columns'],
			$this->tables[$tableName]['indices']);
	}

	protected function _updateConfigTable($tableName, $data, $incremental = true)
	{
		if (!isset($this->tables[$tableName])) return false;

		$result = $this->_saveTable(
			$this->bucketId . '.' . $tableName,
			$this->tables[$tableName]['primaryKey'],
			$this->tables[$tableName]['columns'],
			$data,
			$incremental,
			true,
			$this->tables[$tableName]['indices']
		);

		$this->clearCache();
		return $result;
	}

	protected function _updateConfigTableRow($tableName, $data)
	{
		if (!isset($this->tables[$tableName])) return false;

		$result = $this->_updateTableRow(
			$this->bucketId . '.' . $tableName,
			$this->tables[$tableName]['primaryKey'],
			$data,
			$this->tables[$tableName]['indices']
		);

		$this->clearCache();
		return $result;
	}

	protected function _getConfigTableRow($tableName, $id)
	{
		if (!isset($this->tables[$tableName])) return false;

		$result = $this->_fetchTableRows($this->bucketId . '.' . $tableName, $this->tables[$tableName]['primaryKey'], $id);
		return count($result) ? current($result) : false;
	}

	protected function _checkConfigTable($tableName, $columns)
	{
		if (!isset($this->tables[$tableName])) return false;

		if ($columns != $this->tables[$tableName]['columns']) {
			throw new WrongConfigurationException(sprintf("Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
				$tableName, implode(',', $columns), implode(',', $this->tables[$tableName]['columns'])));
		}

		return true;
	}

	/**
	 * @param $tableName
	 * @return array|bool
	 */
	protected function _getConfigTable($tableName)
	{
		if (!isset($this->tables[$tableName])) return false;

		try {
			$table = $this->_fetchTableRows($this->bucketId . '.' . $tableName);
		} catch (ClientException $e) {
			if ($e->getCode() == 404) {
				$this->_createConfigTable($tableName);
				$table = array();
			} else {
				throw $e;
			}
		}

		if (count($table)) {
			$this->_checkConfigTable($tableName, array_keys(current($table)));
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
		$this->_cache = array();
	}

	public function sapi_listBuckets()
	{
		$cacheKey = 'listBuckets';
		if (!isset($this->_cache[$cacheKey])) {
			$this->_cache[$cacheKey] = $this->_storageApiClient->listBuckets();
		}
		return $this->_cache[$cacheKey];
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
		if (!isset($this->_cache[$cacheKey])) {
			$this->_cache[$cacheKey] = $this->_storageApiClient->listTables($bucketId, array('include' => ''));
		}
		return $this->_cache[$cacheKey];
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
		if (!isset($this->_cache[$cacheKey])) {
			$this->_cache[$cacheKey] = $this->_storageApiClient->getTable($tableId);
		}
		return $this->_cache[$cacheKey];
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
		if (!isset($this->_cache[$cacheKey]) || !$cache) {
			$csv = $this->_storageApiClient->exportTable($tableId, null, $options);
			$this->_cache[$cacheKey] = StorageApiClient::parseCsv($csv, true);
		}
		return $this->_cache[$cacheKey];
	}
}