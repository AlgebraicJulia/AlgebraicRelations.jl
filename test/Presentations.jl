using AlgebraicRelations.Presentations
using Catlab

# Initialize workflow object
wf = Presentation()

# Add Products to workflow
Files, Images, NeuralNet,
       Accuracy, Metadata = add_types!(wf, [(:Files, String),
                                               (:Images, String),
                                               (:NeuralNet, String),
                                               (:Accuracy, Real),
                                               (:Metadata, String)]);

# Add Processes to workflow
extract, split_im, train, evaluate = add_processes!(wf, [(:extract, Files, Images),
                                                         (:split_im, Images, Images⊗Images),
                                                         (:train, NeuralNet⊗Images, NeuralNet⊗Metadata),
                                                         (:evaluate, NeuralNet⊗Images, Accuracy⊗Metadata)]);
# Convert to Schema
TrainDB = present_to_schema(wf);
draw_schema(wf)

@test typeof(wf) <: Catlab.Present.Presentation
