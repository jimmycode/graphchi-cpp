
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 * @modified Yuxiang WU <yuxiang.cs@gmail.com>
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
 * k-nearest neighbors on probabilistic graph
 */


#include <string>
#include <set>
#include <vector>
#include <random>
#include "graphchi_basic_includes.hpp"

using namespace graphchi;

/* Data structure to record the expected reliability of each vertex */
struct vertex_value {
    float sum; // sum of all shortest path length
    int count; // counter of shortest path reached
};

struct compare_vertex_value {
    bool operator () (const vertex_value& v1, const vertex_value& v2) {
        return v1.sum / v1.count > v2.sum / v2.count;
    }
};

std::set<vertex_value, compare_vertex_value> visited;

/* Data structures for Dijkstra samplings */
struct vertex_sp {
    vid_t id;
    float sp;
};

struct sampling {
    struct compare_sp {
        bool operator () (const vertex_sp& v1, const vertex_sp& v2) {
            return v1.sp > v2.sp;
        }
    };
    
    std::set<vertex_sp, compare_sp> queue;
    std::set<vid_t> visited;
};

std::vector<sampling> samplings;

/* Edge type and vertex type definition */
struct edge_datatype{
    float p; // probability of existence
    const static float w = 1.0; // weight
    int i; // index of iteration
    float s; // shortest path from source to endpoint, including the weight of current edge
};
typedef struct empty {} VertexDataType; // vertex data type is empty
typedef edge_datatype EdgeDataType;

/* Input parameter: source vertex id */
vid_t src;

/**
  * GraphChi programs need to subclass GraphChiProgram<vertex-type, edge-type> 
  * class. The main logic is usually in the update function.
  */
struct kNN : public GraphChiProgram<VertexDataType, EdgeDataType> {
 
    /**
     *  Vertex update function.
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {

        if (gcontext.iteration == 0) {
            /* Deactivate all the vertices, because the scheduler auto activate all 
             * the vertices before first iteration 
             */
            gcontext.scheduler->remove_tasks(vertex.id(), vertex.id());
        } else {
            /* if source vertex get activated, it means a new sampling begins */
            if(vertex.id() == src) {
                sampling new_sampling;
                for(int i=0; i < vertex.num_edges(); i++) {
                    std::default_random_engine generator;
                    std::uniform_real_distribution<float> distribution(0,1);
                    float rand_val = distribution(generator); 
                    if(rand_val < vertex.edge(i).get_data()) {
                        vertex_sp new_vertex;
                        new_vertex.id = vertex.edge(i).vertex_id();
                        new_vertex.sp = vertex.edge(i).get_data().w;
                        samplings.queue.add();
                    }
                }
                samplings.push_back(new_sampling);
            }
            /* Loop over all edges (ignore direction) */
            for(int i=0; i < vertex.num_edges(); i++) {
                // vertex.edge(i).get_data() 
            }
            
            // v.set_data(new_value);
        }
    }
    
    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &gcontext) {
        if(iteration == 1) {
            gcontext.scheduler->add_task(src);
        }
    }
    
    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &gcontext) {
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
    metrics m("kNN");
    
    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 4); // Number of iterations
    bool scheduler       = true; // Whether to use selective scheduling
    
    /* Detect the number of shards or preprocess an input to create them */
    int nshards          = convert_if_notexists<EdgeDataType>(filename, 
                                                            get_option_string("nshards", "auto"));
    
    /* Run */
    kNN program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    engine.run(program, niters);
    
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
