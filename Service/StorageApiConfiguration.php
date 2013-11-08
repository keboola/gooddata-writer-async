<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-09-09
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

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
	protected static $_tables = array();

	/**
	 * @var string
	 */
	public $bucketId;

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 * @param array $options
	 * @return array
	 */
	protected function _fetchTableRows($tableId, $whereColumn = null, $whereValue = null, $options = array())
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
		$csv = $this->_storageApiClient->exportTable($tableId, null, $exportOptions);
		return StorageApiClient::parseCsv($csv, true);
	}

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 */
	protected function _deleteTableRow($tableId, $whereColumn, $whereValue)
	{
		$options = array(
			'whereColumn' => $whereColumn,
			'whereValues' => array($whereValue)
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
		if (!isset(self::$_tables[$tableName])) return false;

		return $this->_createTable(
			$this->bucketId . '.' . $tableName,
			self::$_tables[$tableName]['primaryKey'],
			self::$_tables[$tableName]['columns'],
			self::$_tables[$tableName]['indices']);
	}

	protected function _updateConfigTable($tableName, $data, $incremental = true)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		return $this->_saveTable(
			$this->bucketId . '.' . $tableName,
			self::$_tables[$tableName]['primaryKey'],
			self::$_tables[$tableName]['columns'],
			$data,
			$incremental,
			true,
			self::$_tables[$tableName]['indices']
		);
	}

	protected function _updateConfigTableRow($tableName, $data)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		return $this->_updateTableRow(
			$this->bucketId . '.' . $tableName,
			self::$_tables[$tableName]['primaryKey'],
			$data,
			self::$_tables[$tableName]['indices']
		);
	}

	protected function _getConfigTableRow($tableName, $id)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		$result = $this->_fetchTableRows($this->bucketId . '.' . $tableName, self::$_tables[$tableName]['primaryKey'], $id);
		return count($result) ? current($result) : false;
	}

	protected static function _checkConfigTable($tableName, $columns)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		if ($columns != self::$_tables[$tableName]['columns']) {
			throw new WrongConfigurationException(sprintf("Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
				$tableName, implode(',', $columns), implode(',', self::$_tables[$tableName]['columns'])));
		}

		return true;
	}

	/**
	 * @param $tableName
	 * @return array|bool
	 */
	public function getConfigTable($tableName)
	{
		if (!isset(self::$_tables[$tableName])) return false;

		$tableId = $this->bucketId . '.' . $tableName;
		if (!$this->_storageApiClient->tableExists($tableId)) {
			$this->_createConfigTable($tableName);
		}
		$table = $this->_fetchTableRows($tableId);
		self::_checkConfigTable($tableName, array_keys(current($table)));

		return $table;
	}
}