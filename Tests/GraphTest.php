<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests;

use Keboola\GoodDataWriter\DependencyInjection\Configuration;
use Keboola\GoodDataWriter\Model\Graph;
use Keboola\GoodDataWriter\Exception\GraphTtlException;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

class GraphTest extends WebTestCase
{



	/**
	 * Setup called before every test
	 */
	protected function setUp()
	{
		$this->configuration = new Configuration();
	}

	public function testAddTransition()
	{
		$graph = new Graph();
		$paths = array();
		$graph->addTransition($paths, 'A', 'B');
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'B',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'A',
									'target' => 'B',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
		$graph->addTransition($paths, 'A', 'D');
		$graph->addTransition($paths, 'A', 'C');
		$graph->addTransition($paths, 'C', 'D');
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'B',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'A',
									'target' => 'B',
								),
						),
				),
			1 =>
				array (
					'source' => 'C',
					'target' => 'D',
					'final' => false,
					'partial' => true,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'C',
									'target' => 'D',
								),
						),
				),
			2 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'A',
									'target' => 'D',
								),
						),
				),
			3 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => false,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'C',
									'target' => 'D',
								),
							1 =>
								array (
									'source' => 'A',
									'target' => 'C',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
	}


	public function testStartPoints()
	{
		$graph = new Graph();
		$transitions = array(
			array (
				'source' => 'C',
				'target' => 'D',
			),
			array (
				'source' => 'A',
				'target' => 'C',
			),
			array (
				'source' => 'A',
				'target' => 'D',
			),
			array (
				'source' => 'C',
				'target' => 'D',
			)
		);
		$this->assertEquals(array('A'), $graph->getStartPoints($transitions));
	}

	public function testEndPoints()
	{
		$graph = new Graph();
		$transitions = array(
			array (
				'source' => 'A',
				'target' => 'C',
			),
			array (
				'source' => 'A',
				'target' => 'D',
			),
			array (
				'source' => 'C',
				'target' => 'D',
			)
		);
		$this->assertEquals(array('D'), $graph->getEndPoints($transitions));
	}

	public function testTransitivePaths()
	{
		$graph = new Graph();
		$paths = array();
		$graph->addTransition($paths, '0', 'A');
		$graph->addTransition($paths, 'A', 'B');
		$graph->addTransition($paths, 'A', 'C');
		$graph->addTransition($paths, 'A', 'D');
		$graph->addTransition($paths, 'C', 'D');
		$graph->addTransition($paths, 'D', 'E');
		$transitivePaths = $graph->getTransitivePaths($paths);
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'D',
				),
			1 =>
				array (
					'source' => 'C',
					'target' => 'D',
				),
			2 =>
				array (
					'source' => 'A',
					'target' => 'C',
				),
		);
		$this->assertEquals($expected, $transitivePaths);
	}

	public function testFindRoutes()
	{
		$graph = new Graph();
		$paths = array();
		$transitions = array(
			array (
				'source' => 'A',
				'target' => 'B',
			),
			array (
				'source' => 'A',
				'target' => 'C',
			),
			array (
				'source' => 'A',
				'target' => 'D',
			),
			array (
				'source' => 'C',
				'target' => 'D',
			),
		);
		$graph->findRoutes('B', $transitions, $paths);
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'B',
					'final' => true,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'A',
									'target' => 'B',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
		$paths = array();
		$graph->findRoutes('D', $transitions, $paths);
		$expected = array (
			0 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => true,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'A',
									'target' => 'D',
								),
						),
				),
			1 =>
				array (
					'source' => 'C',
					'target' => 'D',
					'final' => false,
					'partial' => true,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'C',
									'target' => 'D',
								),
						),
				),
			2 =>
				array (
					'source' => 'A',
					'target' => 'D',
					'final' => true,
					'partial' => false,
					'path' =>
						array (
							0 =>
								array (
									'source' => 'C',
									'target' => 'D',
								),
							1 =>
								array (
									'source' => 'A',
									'target' => 'C',
								),
						),
				),
		);
		$this->assertEquals($expected, $paths);
	}

	/**
	 * @expectedException \Keboola\GoodDataWriter\Exception\GraphTtlException
	 */
	public function testFindRoutesTtl()
	{
		$graph = new Graph();
		$paths = array();
		$transitions = array(
			array (
				'source' => 'A',
				'target' => 'B',
			),
			array (
				'source' => 'A',
				'target' => 'C',
			),
			array (
				'source' => 'A',
				'target' => 'D',
			),
			array (
				'source' => 'C',
				'target' => 'D',
			),
		);
		$graph->findRoutes('D', $transitions, $paths, 1);
	}

}
