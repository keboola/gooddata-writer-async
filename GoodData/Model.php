<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 30.01.14
 * Time: 11:03
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Writer\AppConfiguration;

class Model
{

	const TIME_DIMENSION_MANIFEST = 'time-dimension-manifest.json';
	const TIME_DIMENSION_MODEL = 'time-dimension-ldm.json';
	const API_TIME_ZONE = 'Europe/Prague';

	public function __construct(AppConfiguration $appConfiguration)
	{
		$this->timeDimensionManifestPath = $appConfiguration->scriptsPath . '/' . self::TIME_DIMENSION_MANIFEST;
		$this->timeDimensionModelPath = $appConfiguration->scriptsPath . '/' . self::TIME_DIMENSION_MODEL;
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

	public static function getDateDimensionId($name)
	{
		return self::getId($name) . '.dataset.dt';
	}

	public static function getTimeDimensionId($name)
	{
		return 'dataset.time.' . self::getId($name);
	}

	public static function getDatasetId($name)
	{
		return 'dataset.' . self::getId($name);
	}

	public static function getTimestampFromApiDate($date)
	{
		$date = new \DateTime($date, new \DateTimeZone(self::API_TIME_ZONE));
		return $date->getTimestamp();
	}


	/**
 	 * Create json for LDM model manipulation
 	*/
	public function getLDM($tableDefinition)
	{
		$dataSetId = self::getId($tableDefinition['name']);
		// add default connection point
		$dataSet = array(
			'identifier' => self::getDatasetId($tableDefinition['name']),
			'title' => $tableDefinition['name'],
			'anchor' => array(
				'attribute' => array(
					'identifier' => sprintf('attr.%s.factsof', $dataSetId),
					'title' => sprintf('Records of %s', $tableDefinition['name'])
				)
			)
		);

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
							'identifier' => sprintf('attr.%s.%s', $dataSetId, $columnIdentifier),
							'title' => $column['title'],
							'folder' => $tableDefinition['name']
						)
					);

					$label = array(
						'identifier' => sprintf('label.%s.%s', $dataSetId, $columnIdentifier),
						'title' => $column['title'],
						'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
					);

					if (!empty($column['dataType'])) {
						$label['dataType'] = $column['dataType'];
						if (!empty($column['dataTypeSize'])) {
							$label['dataType'] .= '(' . $column['dataTypeSize'] . ')';
						}
					}

					$labels[$column['name']][] = array(
						'label' => $label
					);
					break;
				case 'FACT' :
					$fact = array(
						'fact' => array(
							'identifier' => sprintf('fact.%s.%s', $dataSetId, $columnIdentifier),
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
					$defaultLabelId = sprintf('label.%s.%s', $dataSetId, $columnIdentifier);
					$attribute = array(
						'identifier' => sprintf('attr.%s.%s', $dataSetId, $columnIdentifier),
						'title' => $column['title'],
						'defaultLabel' => $defaultLabelId,
						'folder' => $tableDefinition['name']
					);

					if (!empty($column['sortLabel'])) {
						$attribute['sortOrder'] = array(
							'attributeSortOrder' => array(
								'label' => sprintf('label.%s.%s.%s', $dataSetId, $columnIdentifier, self::getId($column['sortLabel'])),
								'direction' => (!empty($column['sortOrder']) && $column['sortOrder'] == 'DESC') ? 'DESC' : 'ASC'
							)
						);
					}
					$attributes[$column['name']] = array('attribute' => $attribute);

					$label = array(
						'identifier' => $defaultLabelId,
						'title' => $column['title'],
						'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
					);
					if (!empty($column['dataType'])) {
						$label['dataType'] = $column['dataType'];
						if (!empty($column['dataTypeSize'])) {
							$label['dataType'] .= '(' . $column['dataTypeSize'] . ')';
						}
					}
					$labels[$column['name']][] = array(
						'label' => $label
					);
					break;
				case 'HYPERLINK' :
				case 'LABEL' :
					if (!isset($labels[$column['reference']])) {
						$labels[$column['reference']] = array();
					}
					$label = array(
						'identifier' => sprintf('label.%s.%s.%s', $dataSetId, self::getId($column['reference']), $columnIdentifier),
						'title' => $column['title'],
						'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
					);
					if (!empty($column['dataType'])) {
						$label['dataType'] = $column['dataType'];
						if (!empty($column['dataTypeSize'])) {
							$label['dataType'] .= '(' . $column['dataTypeSize'] . ')';
						}
					}
					$labels[$column['reference']][] = array(
						'label' => $label
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
								'identifier' => sprintf('dt.%s.%s', $dataSetId, $columnIdentifier),
								'title' => sprintf('%s Date', $column['title'], $tableDefinition['name']),
								'dataType' => 'INT'
							)
						);
						$facts[] = array(
							'fact' => array(
								'identifier' => sprintf('tm.dt.%s.%s', $dataSetId, $columnIdentifier),
								'title' => sprintf('%s Time', $column['title'], $tableDefinition['name']),
								'dataType' => 'INT'
							)
						);
					}
					$references[] = self::getId($column['schemaReference']) . (!empty($column['template']) ? '.' . $column['template'] : null);
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
							sprintf('%s.date.mmddyyyy', $dimensionName . (!empty($column['template']) ? '.' . strtolower($column['template']) : null))
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