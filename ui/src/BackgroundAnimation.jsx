import React, { useEffect, useRef } from 'react';

const BackgroundAnimation = () => {
    const canvasRef = useRef(null);
    const mouseRef = useRef({ x: -1000, y: -1000 });

    useEffect(() => {
        const canvas = canvasRef.current;
        const ctx = canvas.getContext('2d');
        let animationFrameId;

        const resize = () => {
            canvas.width = window.innerWidth;
            canvas.height = window.innerHeight;
        };

        window.addEventListener('resize', resize);
        resize();

        // City points (Nodes)
        const nodes = [];
        const nodeCount = 50; // Further increased for even more coverage

        const createNodes = () => {
            nodes.length = 0;
            for (let i = 0; i < nodeCount; i++) {
                nodes.push({
                    x: Math.random() * canvas.width,
                    y: Math.random() * canvas.height,
                    vx: (Math.random() - 0.5) * 0.12,
                    vy: (Math.random() - 0.5) * 0.12,
                    baseRadius: Math.random() * 2 + 1.2,
                    radius: 0,
                    pulse: Math.random() * Math.PI * 2,
                });
            }
        };

        createNodes();

        // Cargo particles (Data Packets)
        const particles = [];
        const particleCount = 70;

        class Particle {
            constructor() {
                this.reset();
            }

            reset() {
                this.startNode = nodes[Math.floor(Math.random() * nodes.length)];
                this.endNode = nodes[Math.floor(Math.random() * nodes.length)];
                while (this.endNode === this.startNode) {
                    this.endNode = nodes[Math.floor(Math.random() * nodes.length)];
                }
                this.progress = 0;
                this.speed = Math.random() * 0.0012 + 0.0004;
                this.x = this.startNode.x;
                this.y = this.startNode.y;
                this.size = Math.random() * 1.2 + 0.8;
            }

            update() {
                this.progress += this.speed;
                if (this.progress >= 1) {
                    this.reset();
                }

                const targetX = this.startNode.x + (this.endNode.x - this.startNode.x) * this.progress;
                const targetY = this.startNode.y + (this.endNode.y - this.startNode.y) * this.progress;

                const dx = mouseRef.current.x - targetX;
                const dy = mouseRef.current.y - targetY;
                const dist = Math.sqrt(dx * dx + dy * dy);
                const pullStrength = 180;

                if (dist < pullStrength) {
                    const power = (pullStrength - dist) / pullStrength;
                    this.x = targetX + dx * power * 0.25;
                    this.y = targetY + dy * power * 0.25;
                } else {
                    this.x = targetX;
                    this.y = targetY;
                }
            }

            draw(globalFactor) {
                // Radial fade for particles
                const nodeDistX = this.x - canvas.width / 2;
                const nodeDistY = this.y - canvas.height / 2;
                const centerDist = Math.sqrt(nodeDistX * nodeDistX + nodeDistY * nodeDistY);
                const maxDist = Math.sqrt(Math.pow(canvas.width / 2, 2) + Math.pow(canvas.height / 2, 2));
                const radialFactor = Math.max(0.1, 1 - (centerDist / (maxDist * 0.9)));

                ctx.beginPath();
                ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
                ctx.fillStyle = `rgba(143, 162, 144, ${0.7 * radialFactor})`;
                ctx.fill();
            }
        }

        for (let i = 0; i < particleCount; i++) {
            particles.push(new Particle());
        }

        const animate = () => {
            ctx.clearRect(0, 0, canvas.width, canvas.height);

            const centerX = canvas.width / 2;
            const centerY = canvas.height / 2;
            const maxRadius = Math.sqrt(Math.pow(centerX, 2) + Math.pow(centerY, 2));

            // Update nodes
            nodes.forEach(node => {
                node.x += node.vx;
                node.y += node.vy;
                node.pulse += 0.04;

                if (node.x < 0 || node.x > canvas.width) node.vx *= -1;
                if (node.y < 0 || node.y > canvas.height) node.vy *= -1;

                const nodeDistX = node.x - centerX;
                const nodeDistY = node.y - centerY;
                const centerDist = Math.sqrt(nodeDistX * nodeDistX + nodeDistY * nodeDistY);
                const radialFactor = Math.max(0.1, 1 - (centerDist / (maxRadius * 0.85)));

                const mouseDx = mouseRef.current.x - node.x;
                const mouseDy = mouseRef.current.y - node.y;
                const mouseDist = Math.sqrt(mouseDx * mouseDx + mouseDy * mouseDy);
                const hoverScale = mouseDist < 150 ? (150 - mouseDist) / 150 * 3 : 0;

                node.radius = node.baseRadius + Math.sin(node.pulse) * 0.4 + hoverScale;

                ctx.beginPath();
                ctx.arc(node.x, node.y, node.radius, 0, Math.PI * 2);
                const opacity = Math.min(0.6, (mouseDist < 150 ? 0.6 : 0.25 * radialFactor));
                ctx.fillStyle = `rgba(143, 162, 144, ${opacity})`;
                ctx.fill();
            });

            // Draw pathways
            ctx.lineWidth = 0.7;
            for (let i = 0; i < nodes.length; i++) {
                for (let j = i + 1; j < nodes.length; j++) {
                    const n1 = nodes[i];
                    const n2 = nodes[j];
                    const dist = Math.sqrt(Math.pow(n1.x - n2.x, 2) + Math.pow(n1.y - n2.y, 2));

                    if (dist < 320) {
                        const midX = (n1.x + n2.x) / 2;
                        const midY = (n1.y + n2.y) / 2;
                        const centerDist = Math.sqrt(Math.pow(midX - centerX, 2) + Math.pow(midY - centerY, 2));
                        const radialFactor = Math.max(0.05, 1 - (centerDist / (maxRadius * 0.8)));

                        const mouseDist1 = Math.sqrt(Math.pow(mouseRef.current.x - n1.x, 2) + Math.pow(mouseRef.current.y - n1.y, 2));
                        const mouseDist2 = Math.sqrt(Math.pow(mouseRef.current.x - n2.x, 2) + Math.pow(mouseRef.current.y - n2.y, 2));

                        const isHighlighted = mouseDist1 < 150 || mouseDist2 < 150;
                        const baseOpacity = isHighlighted ? 0.3 : 0.08 * (1 - dist / 320) * radialFactor;

                        ctx.beginPath();
                        ctx.strokeStyle = `rgba(143, 162, 144, ${baseOpacity})`;
                        ctx.moveTo(n1.x, n1.y);
                        ctx.lineTo(n2.x, n2.y);
                        ctx.stroke();
                    }
                }
            }

            // Update and draw particles
            particles.forEach(p => {
                p.update();
                p.draw();
            });

            animationFrameId = requestAnimationFrame(animate);
        };

        animate();

        const handleMouseMove = (e) => {
            mouseRef.current = { x: e.clientX, y: e.clientY };
        };

        window.addEventListener('mousemove', handleMouseMove);

        return () => {
            window.removeEventListener('resize', resize);
            window.removeEventListener('mousemove', handleMouseMove);
            cancelAnimationFrame(animationFrameId);
        };
    }, []);

    return (
        <canvas
            ref={canvasRef}
            className="hero-background-canvas"
            style={{
                position: 'fixed',
                top: 0,
                left: 0,
                width: '100%',
                height: '100%',
                zIndex: -1,
                pointerEvents: 'none',
                opacity: 0.9,
            }}
        />
    );
};

export default BackgroundAnimation;
