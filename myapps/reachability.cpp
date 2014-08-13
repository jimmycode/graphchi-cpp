
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 * @modified  Yuxiang Wu <yuxiang.cs@gmail.com>
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
 *
 * @section DESCRIPTION
 *
 * Program for computing reachability between two vertices in a directed graph
 *  */



#include <string>

#include "graphchi_basic_includes.hpp"

using namespace graphchi;

/**
  * Type definitions. Remember to create suitable graph shards using the
  * Sharder-program. 
  */
typedef vid_t VertexDataType; // vid_t is the vertex id type
typedef vid_t EdgeDataType; // no data on edge

/**
  * Input parameters: query source and destination
  */
VertexDataType query_src;
VertexDataType query_dst;

/**
  * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
  * class. The main logic is usually in the update function.
  */
struct ReachabilityProgram : public GraphChiProgram<VertexDataType, EdgeDataType> {
 
    bool terminate;
    bool converged;

    /**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {

        if (gcontext.iteration == 0) {
            /* On first iteration, initialize vertex (and its edges). This is usually required, because
               on each run, GraphChi will modify the data files. To start from scratch, it is easiest
               do initialize the program in code. Alternatively, you can keep a copy of initial data files. */
            vertex.set_data(vertex.id());

            gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
            gcontext.scheduler->add_task(query_src);
            converged = false;
        } else {
            /* For source vertex, propagate its id to neighbors */ 
            if(vertex.id() == query_src) {
                for(int i = 0; i < vertex.num_outedges(); i++) {
                    vertex.outedge(i)->set_data(query_src);
                    vid_t nlabel = vertex.outedge(i)->vertex_id();
                    gcontext.scheduler->add_task(nlabel);
                }
                converged = false;
                gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
            }
            /* If it reaches the destination */
            else if(vertex.id() == query_dst) {
                for(int i = 0; i < vertex.num_inedges(); i++) {
                    if(vertex.inedge(i)->get_data() == query_src) {
                        // terminate the program
                        terminate = true;
                    }
                }
            }
            /* Already visited vertices */
            else if(vertex.get_data() == query_src) {
                gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
            }
            /* For other vertices, if one of its inedge's value is query_src, propagate 
               query_src to its outedges*/
            else {
                for(int i = 0; i < vertex.num_inedges(); i++) {
                    if(vertex.inedge(i)->get_data() == query_src) {
                        for(int i=0; i < vertex.num_outedges(); i++) {
                            vertex.outedge(i)->set_data(query_src);
                            vid_t nlabel = vertex.outedge(i)->vertex_id();
                            gcontext.scheduler->add_task(nlabel);
                        }
                        converged = false;
                        break;
                    }
                }
                gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
            }
            
        }
    }
    
    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &gcontext) {
        terminate = false;
        converged = true;
    }
    
    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &gcontext) {
        if (terminate) {
            std::cout << "Connected" << std::endl;
            gcontext.set_last_iteration(iteration);
        }
        else if (converged) {
            std::cout << "Converged, not Connected" << std::endl;
            gcontext.set_last_iteration(iteration);
        }
    }
    
    /**
     * Called before an execution interval is started.
     */
    void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {        
    }
    
    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {        
    }
    
};

int main(int argc, const char ** argv) {
    /* GraphChi initialization will read the command line 
       arguments and the configuration file. */
    graphchi_init(argc, argv);
    
    /* Metrics object for keeping track of performance counters
       and other information. Currently required. */
    metrics m("reachability");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 1000); // Number of iterations
    bool scheduler       = get_option_int("scheduler", 1); // Whether to use selective scheduling

    /* Get query source and destination */
    query_src = get_option_int("source");
    query_dst = get_option_int("dest");
    
    /* Detect the number of shards or preprocess an input to create them */
    int nshards          = convert_if_notexists<EdgeDataType>(filename, 
                                                            get_option_string("nshards", "auto"));
    
    /* Run */
    ReachabilityProgram program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
    engine.run(program, niters);
    
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
