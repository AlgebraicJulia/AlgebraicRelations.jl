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
extract, split_im, train, test_net = add_processes!(wf, [(:extract, Files, Images),
                                                         (:split_im, Images, Images⊗Images),
                                                         (:train, NeuralNet⊗Images, NeuralNet⊗Metadata),
                                                         (:test_net, NeuralNet⊗Images, Accuracy⊗Metadata)]);
# Convert to Schema
TrainDB = present_to_schema(wf);
g = draw_schema(wf)

@test wf isa Catlab.Present.Presentation
@test TrainDB <: Catlab.CategoricalAlgebra.ACSet
@test g isa Catlab.Graphics.Graphviz.Graph

@testset "Scheduling with Real Math" begin
math = Presentation()

# Add Products to workflow
Num, = add_types!(math, [(:Num, Int)]);

# Add Processes to workflow
add, sub, mult, div = add_processes!(math, [(:add, Num⊗Num, Num),
                                          (:sub, Num⊗Num, Num),
                                          (:mult, Num⊗Num, Num),
                                          (:div, Num⊗Num, Num)]);
opers = @program math (x::Num, y::Num, z::Num) begin
    x_p_y = add(x,y)
    total = add(x_p_y, z)
    div_z = div(total, z)
    return div_z
end;

@test evaluate(opers, Dict(:add=> +, :sub=> -, :div => /, :mult => *), [1,2,3])[1] == 2.0
end

@testset "Scheduling with Imaginary Math" begin
im_math = Presentation()

# Add Products to workflow
Num, = add_types!(im_math, [(:Num, Int)]);

# Add Processes to workflow
add, sub, mult, div, neg = add_processes!(im_math, [(:add, Num⊗Num⊗Num⊗Num, Num⊗Num),
                                          (:sub, Num⊗Num⊗Num⊗Num, Num⊗Num),
                                          (:mult, Num⊗Num⊗Num⊗Num, Num⊗Num),
                                          (:div, Num⊗Num⊗Num⊗Num, Num⊗Num),
                                          (:sc_div, Num⊗Num⊗Num, Num⊗Num),
                                          (:neg, Num, Num)]);

im_add(xr, xi, yr, yi) = (xr+yr, xi+yi)
im_sub(xr, xi, yr, yi) = (xr-yr, xi-yi)
im_mult(xr, xi, yr, yi) = (xr*yr - xi*yi, xr*yi+xi*yr)
sc_div(xr, xi, c) = (xr/c, xi/c)
sc_neg(x) = -x
im_div_p = @program im_math (xr::Num, xi::Num, yr::Num, yi::Num) begin
    numr, numi = mult(xr, xi, yr, neg(yi))
    denr, deni = mult(yr, yi, yr, neg(yi))
    return sc_div(numr, numi, denr)
end
func_map = Dict(:add=>im_add, :sub=>im_sub, :mult=>im_mult, :sc_div=>sc_div, :neg=>sc_neg)
im_div(xr, xi, yr, yi) = evaluate(im_div_p, func_map, [xr, xi, yr, yi])
func_map[:div] = im_div

im_test = @program im_math (xr::Num, xi::Num, yr::Num, yi::Num) begin
  sumr, sumi = add(xr, xi, yr, yi)
  swxr, swxi = div(xi, xr, sumi, xr)
  swyr, swyi = mult(yi, yr, sumr, yr)
  allsumr, allsumi = add(sumi, sumi, sumi, sumi)
  return swxr, swxi, swyr, swyi
end

@test evaluate(im_test, func_map, [2, 2, 3, 4]) == (0.4, 0.2, 11, 27)

end
