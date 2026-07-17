import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.lines import Line2D

fig, ax = plt.subplots(figsize=(14, 10))
ax.set_xlim(0, 14)
ax.set_ylim(0, 10)
ax.axis('off')

C_CLIENT = "#DCE8FB"
C_LB     = "#F5E3B3"
C_CORE   = "#CDEFD8"
C_STATE  = "#F6C6C6"
C_LOG    = "#E3D6F5"
C_EXT    = "#E0E0E0"
EDGE     = "#333333"

def box(x, y, w, h, text, color, fontsize=9.5, style="round,pad=0.02,rounding_size=0.08", weight="bold"):
    b = FancyBboxPatch((x, y), w, h, boxstyle=style, linewidth=1.3,
                        edgecolor=EDGE, facecolor=color, zorder=2)
    ax.add_patch(b)
    ax.text(x + w/2, y + h/2, text, ha='center', va='center',
            fontsize=fontsize, fontweight=weight, zorder=3, linespacing=1.4)
    return (x, y, w, h)

def arrow(p1, p2, label=None, color=EDGE, style='-|>', ls='solid', rad=0.0, lw=1.4, fontsize=8):
    a = FancyArrowPatch(p1, p2, arrowstyle=style, mutation_scale=14,
                         color=color, linewidth=lw, linestyle=ls,
                         connectionstyle=f"arc3,rad={rad}", zorder=1)
    ax.add_patch(a)
    if label:
        mx, my = (p1[0]+p2[0])/2, (p1[1]+p2[1])/2
        ax.text(mx, my, label, ha='center', va='center', fontsize=fontsize,
                color=color, backgroundcolor='white', zorder=4,
                bbox=dict(boxstyle='round,pad=0.1', fc='white', ec='none'))

ax.text(7, 9.6, "Global Rate Limiter as a Service — Architecture", ha='center',
        fontsize=15, fontweight='bold')

# Row 1: calling microservices
ms1 = box(0.5, 8.2, 2.0, 0.7, "Microservice\nInstance A", C_CLIENT)
ms2 = box(3.0, 8.2, 2.0, 0.7, "Microservice\nInstance B", C_CLIENT)
ms3 = box(5.5, 8.2, 2.0, 0.7, "Microservice\nInstance N", C_CLIENT)
ax.text(4.0, 9.0, "Internal callers (10+ instances per service) asking:\n\u201cAm I allowed to call the 3rd-party API right now?\u201d",
        ha='center', fontsize=8.5, style='italic', color='#555')

# Load balancer
lb = box(3.0, 7.0, 2.0, 0.6, "Load Balancer\n(nginx / L4)", C_LB)
for m in [ms1, ms2, ms3]:
    arrow((m[0]+m[2]/2, m[1]), (lb[0]+lb[2]/2, lb[1]+lb[3]), rad=0.0)

# Rate limiter cluster
rl1 = box(0.6, 5.6, 2.1, 0.9, "RL Node 1\n(stateless)\nlocal fallback\nbucket + breaker", C_CORE, fontsize=8)
rl2 = box(3.0, 5.6, 2.1, 0.9, "RL Node 2\n(stateless)\nlocal fallback\nbucket + breaker", C_CORE, fontsize=8)
rl3 = box(5.4, 5.6, 2.1, 0.9, "RL Node N\n(stateless)\nlocal fallback\nbucket + breaker", C_CORE, fontsize=8)
arrow((lb[0]+0.3, lb[1]), (rl1[0]+1.05, rl1[1]+rl1[3]))
arrow((lb[0]+lb[2]/2, lb[1]), (rl2[0]+1.05, rl2[1]+rl2[3]))
arrow((lb[0]+lb[2]-0.3, lb[1]), (rl3[0]+1.05, rl3[1]+rl3[3]))

# Redis - shared state
redis = box(2.6, 4.0, 2.9, 0.8, "Redis\nToken-bucket state (atomic Lua script)\nshared across all RL nodes", C_STATE, fontsize=8.5)
arrow((rl1[0]+1.05, rl1[1]), (redis[0]+0.5, redis[1]+redis[3]), label="EVALSHA\n(check+consume,\n~sub-ms)", fontsize=7, rad=-0.05)
arrow((rl2[0]+1.05, rl2[1]), (redis[0]+redis[2]/2, redis[1]+redis[3]))
arrow((rl3[0]+1.05, rl3[1]), (redis[0]+redis[2]-0.5, redis[1]+redis[3]), rad=0.05)

# Config store (client limits)
cfg = box(8.6, 5.6, 2.4, 0.9, "Client Config\n(limits per client,\ncached in-process,\nTTL refresh)", C_STATE, fontsize=8)
arrow((rl3[0]+2.1, rl3[1]+0.45), (cfg[0], cfg[1]+0.45), label="reads")

# Circuit breaker fallback note
ax.annotate("Redis unreachable \u2192 circuit opens \u2192\nnodes fail-safe to local in-memory\napproximate bucket (fail-OPEN, degraded)",
            xy=(1.65, 5.6), xytext=(0.1, 3.9), fontsize=7.5, color='#8a1f1f',
            arrowprops=dict(arrowstyle='->', color='#8a1f1f', lw=1.1, connectionstyle="arc3,rad=-0.2"))

# Async logging path
queue = box(3.4, 2.7, 2.2, 0.7, "Async Log Queue\n(in-proc buffer)", C_LOG, fontsize=8.5)
arrow((rl2[0]+1.05, rl2[1]), (queue[0]+1.1, queue[1]+queue[3]), label="fire-and-forget\n(non-blocking)", fontsize=7, rad=0.15)

worker = box(3.4, 1.6, 2.2, 0.7, "Log Worker\n(batched writer)", C_LOG, fontsize=8.5)
arrow((queue[0]+1.1, queue[1]), (worker[0]+1.1, worker[1]+worker[3]))

pg = box(0.6, 0.5, 2.6, 0.7, "PostgreSQL\nrequest_log +\nusage aggregates", C_STATE, fontsize=8)
arrow((worker[0], worker[1]+0.35), (pg[0]+2.6, pg[1]+0.5), rad=0.1)

# Dashboard API + frontend
dapi = box(6.6, 1.6, 2.4, 0.7, "Dashboard API\n(filters: avg latency,\n10/15/30-day trend)", C_LOG, fontsize=8)
arrow((pg[0]+2.6, pg[1]+0.35), (dapi[0], dapi[1]+0.35), rad=-0.15, label="reads\naggregates")

dash = box(9.6, 1.6, 2.2, 0.7, "Client Dashboard\n(web UI)", C_CLIENT, fontsize=8.5)
arrow((dapi[0]+2.4, dapi[1]+0.35), (dash[0], dash[1]+0.35))

# External APIs, after approval
ext = box(9.4, 5.6, 3.0, 0.9, "3rd-Party APIs\n(banking, logistics,\nAI providers \u2014 strict quotas)", C_EXT, fontsize=8)
arrow((rl3[0]+2.1, rl3[1]+0.7), (ext[0], ext[1]+0.45), label="on ALLOW,\ncaller proceeds", fontsize=7)

# Legend
legend_elems = [
    Line2D([0],[0], color=EDGE, lw=1.4, label='Request path (check)'),
    Line2D([0],[0], color='#8a1f1f', lw=1.1, linestyle='-', label='Fail-safe path (Redis down)'),
]
ax.legend(handles=legend_elems, loc='lower right', bbox_to_anchor=(1.0, -0.02), fontsize=8, frameon=False)

plt.tight_layout()
plt.savefig('/home/claude/vega-rate-limiter/diagrams/architecture.png', dpi=170, bbox_inches='tight')
print("saved")
