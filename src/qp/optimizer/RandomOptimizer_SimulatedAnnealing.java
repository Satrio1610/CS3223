package qp.optimizer;

import qp.operators.Operator;
import qp.utils.SQLQuery;

// implement Simulated Annealing optimization 
// parameter for annealing follows the one provided by the paper
public class RandomOptimizer_SimulatedAnnealing extends RandomOptimizer {
	
	public RandomOptimizer_SimulatedAnnealing(SQLQuery sqlquery) {
		super(sqlquery);
		// TODO Auto-generated constructor stub
	}

	private float TEMPERATURE_REDUCTION_FACTOR = 0.95f;
	private int NUM_ITERATION_BEFORE_FROZEN = 4; 
	
	@Override
	public Operator getOptimizedPlan() {
		
		RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
		
		numJoin = rip.getNumJoins();
		// set initial state 
		Operator state = rip.prepareInitialPlan();
		modifySchema(state);
		
		// set initial temperature 
		PlanCost pc = new PlanCost();
		int startingCost = pc.getCost(state);
		float temperature = 2 * startingCost; 
		
		// set minimum S as S
		Operator minState = state; 
		int minimumCost = startingCost;
		
		// check if there is a join
		int currentIteration = 0;
		int EQUILIBRIUM = sqlquery.getNumJoin() * 16;
		
		// while is not frozen
		while(temperature >= 1 && currentIteration < NUM_ITERATION_BEFORE_FROZEN) {
			int currentEquilibriumCount = 0;
			
			pc = new PlanCost(); 
			int currentStageMinimumCost = pc.getCost(minState);
			while(currentEquilibriumCount < EQUILIBRIUM) {
				// get random state; 
				Operator neighbourState  = getNeighbor(state);
				
				// get the cost of this random state
				pc = new PlanCost(); 
				int currentNeighborCost = pc.getCost(neighbourState);
				
				pc = new PlanCost();
				int currentStateCost = pc.getCost(state);
				int deltaCost = currentNeighborCost - currentStateCost; 
				
				// if the cost is less than the starting plan, accept
				if(deltaCost <= 0) {
					state = neighbourState; 
					currentStateCost = currentNeighborCost;
				}
				
				// if cost is higher, accept it if the probability is e ^ deltaCost/temperature
				if(deltaCost > 0) {
					float probabilityToAccept = (float)Math.exp(-1 * deltaCost/temperature);	
					float probability = (float)Math.random();
					
					if(probability <= probabilityToAccept) {
						state = neighbourState;
						currentStateCost = currentNeighborCost;
					}
				}
				
				// check if this the new minimum
				if(minimumCost > currentStateCost) {
					minimumCost = currentStateCost; 
					minState = state;
				}
				
				currentEquilibriumCount ++;
			}
			// if for this currentStage there is no change to the minimum cost, then start counting down to freeze the iteration
			if(currentStageMinimumCost == minimumCost) {
				currentIteration++;
			}
			
			temperature *= TEMPERATURE_REDUCTION_FACTOR; 
		}
		return minState;
	}

}
