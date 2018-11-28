// function to style vis.js network nodes
function styleNetworkNodes(node){

	// add label
	node.label = "#"+node.id+", "+node.name;

	// bump font
	node.font = {
		size:20
	};

	// add shadow
	node.shadow = {
      enabled: true,
      color: 'rgba(0,0,0,0.5)',
      size:10,
      x:5,
      y:5
    };

    // set base node parameters
    node.shape = 'box';
    node.shapeProperties = {
		borderRadius: 10
	};
	node.physics = false;
	node.borderWidth = 2;

	// Harvests
	if (node.job_type == 'HarvestOAIJob' || node.job_type == 'HarvestStaticXMLJob' || node.job_type == 'HarvestTabularDataJob'){
		node.color = '#deffde';
	}

	// Transform
	else if (node.job_type == 'TransformJob'){
		node.color = '#fffcde';
	}

	// Merge
	else if (node.job_type == 'MergeJob'){
		node.color = '#e3deff';
	}

	// Publish
	else if (node.job_type == 'PublishJob'){
		node.color = '#def3ff';
	}

	// Analysis
	else if (node.job_type == 'AnalysisJob'){
		node.color = '#e8d3bd';		
	}	

	// override color is job is not valid
	if (!node.is_valid){
		node.color = {
			background:node.color,
			highlight:{
				background:node.color,
				border:'#ff9898'				
			},
			border:'#ff9898'
		};
	}

	// override if job is from external Record Group, visually indicate
	if (node.hasOwnProperty('external_record_group')) {
		
		// update node				
		node.shapeProperties.borderDashes = true;
		node.font.color = '#7c7c7c';


		// update edges
		node_edges = getEdgesOfNode(node.id)									
		node_edges.forEach(function(edge){
			edge.dashes = true;			
			edges.update(edge);
		})

	}

	// override if job is slated for deletion
	if (node.deleted) {
		node.color = '#efefef';

		// gray out all edges to this node									
		node_edges = getEdgesOfNode(node.id)									
		node_edges.forEach(function(edge){										
			edge.color.color = '#efefef';
			edge.font.color = '#efefef';
			edges.update(edge);
		})
	}

}


// function to style vis.js network edges
function styleNetworkEdges(edge){

	// add arrow
	edge.arrows = {
		to:{
			enabled: true,
			scaleFactor:1,
			type:'arrow'
		}
	};

	// edge label components
	edge_label_comps = [];

	// include Validity filters
	if (edge.input_validity_valve == 'valid'){
		edge_label_comps.push('Passed Validation');
	}
	else if (edge.input_validity_valve == 'invalid') {
		edge_label_comps.push('Failed Validation');
	}
	else if (edge.input_validity_valve == 'all') {
		edge_label_comps.push('Ignore Validation');
	}
	
	// add ES query valve if applied
	if (edge.input_es_query_valve){		
		edge_label_comps.push('Mapped Field Filtered');
	}

	// if de-duping, add
	if (edge.filter_dupe_record_ids){		
		edge_label_comps.push('De-Duped');
	}

	// if limited, add
	if (edge.input_numerical_valve){		
		edge_label_comps.push(`Limit (${edge.input_numerical_valve})`);
	}

	// format count
	if (edge.total_records_passed == null) {
		count = '...';
	}
	else {
		count = edge.total_records_passed;
	}

	// join as string	
	edge.label = `(${count}) ` + edge_label_comps.join(', ');

	// all records edge
	if (edge.input_validity_valve == 'all'){
		edge.color = {
			color:'orange'
		};
		edge.font = {
			color:'orange'
		}	
	}
	
	// valid records edge
	else if (edge.input_validity_valve == 'valid'){
		edge.color = {
			color:'green'
		};
		edge.font = {
			color:'green'
		}
	}

	// invalid records edge
	else if (edge.input_validity_valve == 'invalid'){
		edge.color = {
			color:'red'
		};
		edge.font = {
			color:'red'
		}
	}

}


// helper function to get edges of node
function getEdgesOfNode(nodeId) {
		return edges.get().filter(function (edge) {
		return edge.from === nodeId || edge.to === nodeId;
	});
}





