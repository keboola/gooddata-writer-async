<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 30.01.14
 * Time: 11:03
 */

namespace Keboola\GoodDataWriter\GoodData;


class Model
{

	const TIME_DIMENSION_MANIFEST = 'time-dimension-manifest.json';
	const TIME_DIMENSION_MODEL = 'time-dimension-ldm.json';

	public function __construct($scriptsPath)
	{
		$this->timeDimensionManifestPath = $scriptsPath . '/' . self::TIME_DIMENSION_MANIFEST;
		$this->timeDimensionModelPath = $scriptsPath . '/' . self::TIME_DIMENSION_MODEL;
	}

	/**
	 * Create identifier from name
	 */
	public static function getId($name)
	{
		$string = iconv('utf-8', 'ascii//ignore//translit', $name);
		$string = preg_replace('/[^\w\d_]/', '', $string);
		$string = preg_replace('/^[\d_]*/', '', $string);
		return strtolower($string);
	}

	public static function getDimensionId($name)
	{
		return self::getId($name) . '.dataset.dt';
	}

	public static function getDatasetId($name)
	{
		return 'dataset.' . self::getId($name);
	}


	/**
 	 * Create json for LDM model manipulation
 	*/
	public function getLDM($tableDefinition)
	{
		$dataSet = array(
			'identifier' => self::getDatasetId($tableDefinition['name']),
			'title' => $tableDefinition['name']
		);
		$dataSetId = self::getId($tableDefinition['name']);

		$facts = array();
		$attributes = array();
		$references = array();
		$labels = array();
		$connectionPoint = null;
		foreach ($tableDefinition['columns'] as $column) {
			$columnIdentifier = self::getId($column['name']);

			switch($column['type']) {
				case 'CONNECTION_POINT' :
					$connectionPoint = $column['name'];
					$dataSet['anchor'] = array(
						'attribute' => array(
							'identifier' => 'attr.' . $dataSetId . '.' . $columnIdentifier,
							'title' => $column['title'],
							'folder' => $tableDefinition['name']
						)
					);
					$labels[$column['name']][] = array(
						'label' => array(
							'identifier' => 'label.' . $dataSetId . '.' . $columnIdentifier,
							'title' => $column['title'],
							'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
						)
					);
					break;
				case 'FACT' :
					$fact = array(
						'fact' => array(
							'identifier' => 'fact.' . $dataSetId . '.' . $columnIdentifier,
							'title' => $column['title'],
						)
					);
					if (!empty($column['dataType'])) {
						$fact['fact']['dataType'] = $column['dataType'];
						if (!empty($column['dataTypeSize'])) {
							$fact['fact']['dataType'] .= '(' . $column['dataTypeSize'] . ')';
						}
					}
					$facts[] = $fact;
					break;
				case 'ATTRIBUTE' :
					$attributes[$column['name']] = array(
						'attribute' => array(
							'identifier' => 'attr.' . $dataSetId . '.' . $columnIdentifier,
							'title' => $column['title'],
							'folder' => $tableDefinition['name']
						)
					);
					$labels[$column['name']][] = array(
						'label' => array(
							'identifier' => 'label.' . $dataSetId . '.' . $columnIdentifier,
							'title' => $column['title'],
							'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
						)
					);
					break;
				case 'HYPERLINK' :
				case 'LABEL' :
					if (!isset($labels[$column['reference']])) {
						$labels[$column['reference']] = array();
					}
					$labels[$column['reference']][] = array(
						'label' => array(
							'identifier' => 'label.' . $dataSetId . '.' . self::getId($column['reference']) . '.' . $columnIdentifier,
							'title' => $column['title'],
							'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
						)
					);

					break;
				case 'REFERENCE' :
					$references[] = self::getDatasetId($column['schemaReference']);
					break;
				case 'DATE' :
					if ($column['includeTime']) {
						$references[] = 'dataset.time.' . self::getId($column['schemaReference']);
						$facts[] = array(
							'fact' => array(
								'identifier' => 'dt.' . $dataSetId . '.' . $columnIdentifier,
								'title' => sprintf('%s Date', $column['title'], $tableDefinition['name']),
								'dataType' => 'INT'
							)
						);
						$facts[] = array(
							'fact' => array(
								'identifier' => 'tm.dt.' . $dataSetId . '.' . $columnIdentifier,
								'title' => sprintf('%s Time', $column['title'], $tableDefinition['name']),
								'dataType' => 'INT'
							)
						);
					}
					$references[] = self::getId($column['schemaReference']);
					break;
			}
		}

		foreach ($labels as $attributeId => $labelArray) {
			if (isset($attributes[$attributeId])) {
				$attributes[$attributeId]['attribute']['labels'] = $labelArray;
			} else if ($attributeId == $connectionPoint) {
				$dataSet['anchor']['attribute']['labels'] = $labelArray;
			}
		}

		if (count($facts)) {
			$dataSet['facts'] = $facts;
		}
		if (count($attributes)) {
			$dataSet['attributes'] = \array_values($attributes);
		}
		if (count($references)) {
			$dataSet['references'] = $references;
		}

		return $dataSet;
	}


	/**
	 * Create manifest for data load
	 */
	public static function getDataLoadManifest($definition, $incrementalLoad)
	{
		$dataSetName = self::getId($definition['name']);
		$manifest = array(
			'dataSetSLIManifest' => array(
				'file' => 'data.csv',
				'dataSet' => 'dataset.' . $dataSetName,
				'parts' => array()
			)
		);
		foreach ($definition['columns'] as $column) {
			$columnName = self::getId($column['name']);
			switch ($column['type']) {
				case 'CONNECTION_POINT':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'],
						'populates' => array(
							sprintf('label.%s.%s', $dataSetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'FACT':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'],
						'populates' => array(
							sprintf('fact.%s.%s', $dataSetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'ATTRIBUTE':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'],
						'populates' => array(
							sprintf('label.%s.%s', $dataSetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'LABEL':
				case 'HYPERLINK':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'],
						'populates' => array(
							sprintf('label.%s.%s.%s', $dataSetName, self::getId($column['reference']), $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'REFERENCE':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'],
						'populates' => array(
							sprintf('label.%s.%s', self::getId($column['schemaReference']), self::getId($column['reference']))
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'DATE':
					$dimensionName = self::getId($column['schemaReference']);
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'],
						'populates' => array(
							sprintf('%s.date.mmddyyyy', $dimensionName)
						),
						'constraints' => array(
							'date' => (string)$column['format']
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => $column['name'] . '_dt',
						'populates' => array(
							sprintf('dt.%s.%s', $dataSetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					if ($column['includeTime']) {
						$manifest['dataSetSLIManifest']['parts'][] = array(
							'columnName' => $column['name'] . '_tm',
							'populates' => array(
								sprintf('tm.dt.%s.%s', $dataSetName, $columnName)
							),
							'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
						);
						$manifest['dataSetSLIManifest']['parts'][] = array(
							'columnName' => $column['name'] . '_id',
							'populates' => array(
								sprintf('label.time.second.of.day.%s', $dimensionName)
							),
							'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
							'referenceKey' => 1
						);
					}
					break;
				case 'IGNORE':
					break;
			}
		}

		return $manifest;
	}

	/**
	 * Create manifest for data load of time dimension
	 */
	public function getTimeDimensionDataLoadManifest($dimensionName)
	{
		$manifest = file_get_contents($this->timeDimensionManifestPath);
		$manifest = str_replace('%NAME%', self::getId($dimensionName), $manifest);
		return $manifest;
	}
}