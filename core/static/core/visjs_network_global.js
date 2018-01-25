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
	node.borderWidthSelected = 5;

	// Harvests
	if (node.job_type == 'HarvestOAIJob' || node.job_type == 'HarvestStaticXMLJob'){
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
		node.color = '#ff9898';					
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
	}

}






