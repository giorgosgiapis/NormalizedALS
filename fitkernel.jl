using Statistics
using Random
using LinearAlgebra
using SpecialFunctions
using SCS
using JuMP
using Sobol
using Distributions
using Polynomials
using Plots

# Returns the nth Chebyshev polynomial of the first kind
function get_chebyshev(n::Int)
	@assert n >= 0
	return ChebyshevT([[0 for i=0:n-1]; 1])
end

PATH = "D:/ml-latest-small/ml-latest-small/" # Path of the dataset

C = (n) -> 2 * exp((n/2)*log(pi) - loggamma((n/2)))

# For every 0 <= k <= N, approximates \int_{cutoff}^1 T_k(t) (1-t^2)^{(rank-1)/3} dt
function compute_normalization_coefficients(N::Int64, rank::Int64, cutoff::Float64)
	@assert -1 <= cutoff <= 1
	# Approximating the step function
	xs = collect(-1:1e-04:1);
	ys = broadcast(x->(x>=cutoff ? 1 : 0), xs);
	step_function = Polynomial(Polynomials.fit(ChebyshevT, xs, ys, 30)); # We're approximating the step function as a polynomial
	
	monomial_integrals = zeros(30+N+3) # Integrals of the form \int_{-1}^1 t^k (1-t^2)^((rank-1)/3) dt
	for l=0:2:30+N+2
		monomial_integrals[l+1] = beta(l/2+1/2, (rank-1)/2)
	end
	
	
	cutoff_monomial_integrals = zeros(N+1) # Integrals of the form \int_{cutoff}^1  x^t (1-t^2)^((rank-1)/3) dt
	for l=0:N
		poly = coeffs(step_function * Polynomial()^l)
		cutoff_monomial_integrals[l+1] = sum(poly[k] * monomial_integrals[k] for k=1:length(poly))
	end
	
	b_k = zeros(N+1)
	for k=0:N
		chebyshev_coeffs = coeffs(Polynomial(get_chebyshev(k)))
		b_k[k+1] = sum(cutoff_monomial_integrals[k] * chebyshev_coeffs[k] for k=1:length(chebyshev_coeffs))
	end
	return b_k
end

function fit_kernel(training_set::Vector{Vector{Vector{Float64}}}, N::Int, cutoff::Float64)::Tuple{Vector{Float64},Float64}
	rank = length(training_set[1][1])
	Random.seed!(1)
	
	model = JuMP.Model(optimizer_with_attributes(SCS.Optimizer, "eps_abs" => 3e-03, "max_iters" => 15000))
	@variable(model, a[1:N+1]) # a from 0 to N
	
	basis_polys = [Polynomial(get_chebyshev(k)) for k=0:N];
	@constraint(model, sum(a[k] * basis_polys[k](cutoff) for k=1:N+1) == 1e-05)
	
	# Normalization constraint
	normalization_coefficients = compute_normalization_coefficients(N, rank, cutoff)
	@constraint(model, C(rank-1) * dot(a, normalization_coefficients) == 1)
	
	# Monotonicity constraints
	for t=cutoff:0.01:1
		@constraint(model, sum(a[k] * derivative(basis_polys[k])(t) for k=1:N+1) >= 1e-04)
	end
	
	optimization_variables = []
	for _dataset in training_set
		dataset = broadcast(x->x/norm(x), shuffle(_dataset)) # Shuffle and renormalize
		cutoff2 = div(length(dataset)*9,10)
		M_1 = dataset[1:cutoff2]
		M_2 = dataset[cutoff2+1:end]
		
		for y in M_2
			dot_products = [dot(x,y) for x in M_1]
			variable = @variable(model)
			coeffs = [mean((x -> x <= cutoff ? 0 : basis_polys[k](x)).(dot_products)) for k=1:N+1] 
			# variable <= log(coeffs \cdot a) / length(M_2) / length(training_set).
			@constraint(model, [variable * length(M_2) * length(training_set), 1, dot(coeffs,a) + 1e-10] in MOI.ExponentialCone())
			push!(optimization_variables, variable)
		end
	end
	@objective(model, Max, sum(optimization_variables))
	JuMP.optimize!(model)
		
	return JuMP.value.(a), objective_value(model)
end	

# Function for approximately integrating a given function on the sphere in R^n using Sobol' points
function integrate_on_sphere(f, n::Int; minpoints::Int = 1000, maxpoints::Int = 1000000, reltol::Float64 = 1e-02)::Float64
	seq = SobolSeq(n);
	C_n = C(n);
	next!(seq)
	firstevals = [];
	sumevals = 0;
	m = 0
	val_std = -1
	while m <= maxpoints
		p = normalize(broadcast(x->quantile(Normal(), x), next!(seq)))
		v_f = f(p) * C_n
		sumevals += v_f
		m += 1
		if(m < minpoints)
			push!(firstevals, v_f)
		elseif(m==minpoints)
			val_std = std(firstevals)
		elseif(val_std / sqrt(m) < max(abs(sumevals/m), 1) * reltol)
			break
		end
	end
	
	return sumevals/m
end
	

function main()
	if(isempty(ARGS))
		println("Use: fitkernel.jl [inputfile] [outputdir] [initial_cutoff (optional)] [cutoff_step (optional)]")
		return
	end
	
	inputfile = ARGS[1]
	outputdir = ARGS[2]
	initial_cutoff = 0.0;
	cutoff_step = 0.1;
	if(length(ARGS) > 2)
		initial_cutoff = parse(Float64, ARGS[3])
		if(length(args) > 3)
			cutoff_step = parse(Float64, ARGS[4])
		end
	end

	if(!isdir(outputdir))
		mkdir(outputdir)
	end

	datasets = Vector{Vector{Float64}}[]
	for line in readlines(inputfile)
		if(line[1] == '*')
			push!(datasets, Vector{Float64}[])
		else
			push!(datasets[end], normalize(broadcast(x -> parse(Float64, x), split(line, " "))))
		end
	end
	
	shuffle!(datasets)
	k = div(length(datasets) * 4, 5)
	training_datasets = datasets[1:k]
	test_datasets = datasets[k+1:end]
	
	test_losses = [];
	training_losses = [];
	functions = plot(title = "g for every choice of c_0")
	functions_file = open(outputdir * "/polys.txt", "w")
	
	xvals = []
	for cutoff=initial_cutoff:cutoff_step:0.5
		a, training_loss = fit_kernel(training_datasets, 5, cutoff)
		_g = ChebyshevT(a)
		is_nonnegative = true
		for extremum in filter(isreal, roots(derivative(Polynomial(_g))))
			if(typeof(extremum) == ComplexF64)
				extremum = extremum.re
			end
			is_nonnegative &= (extremum <= cutoff || extremum >= 1 || _g(extremum) > -1e-03)
		end
		if(!is_nonnegative)
			println("Nonnegativity constraint violated!")
			continue
		end
			
		g = x -> (x <= cutoff ? 0 : _g(min(x, 1.0)))
		x_0 = datasets[1][1] # Some random vector
		K = (y) -> g(dot(x_0,y))
		if(abs(integrate_on_sphere(K, length(x_0)) - 1) > 0.1) # Make sure that K is a probability distribution on the sphere. This is just a sanity check.
			println("K doesn't integrate to one!")
			continue
		end
		println(functions_file, string(cutoff) * "\t" * join(coeffs(Polynomial(_g)), " ")) # Print the polynomial to file
		push!(xvals, cutoff)
		push!(training_losses, -training_loss)
		plot!(functions, g, 0, 1, label = "c_0=$(cutoff)")
		
		test_loss = 0
		for dataset in test_datasets
			dataset_loss = 0
			threshold = div(length(dataset)*4,5)
			train = dataset[1:threshold]
			test = dataset[threshold+1:end]
			f_KDE = x -> mean(g(dot(x,y)) for y in train)
			for x in test
				dataset_loss -= 1/length(test) * log(max(1e-10, f_KDE(x)))
			end
			test_loss += dataset_loss / length(test_datasets)
		end
		push!(test_losses, test_loss)
	end
	savefig(functions, outputdir * "/functions.png")
	losses_plot = plot(title = "Negative log-likelihood loss for every choice of cutoff")
	scatter!(losses_plot, xvals, test_losses, label = "Test losses")
	scatter!(losses_plot, xvals, training_losses, label = "Training losses")
	savefig(losses_plot, outputdir * "/losses.png")
	close(functions_file)
end


if(contains((@__FILE__), "fitkernel.jl"))
    main()
end
		
	

	
	